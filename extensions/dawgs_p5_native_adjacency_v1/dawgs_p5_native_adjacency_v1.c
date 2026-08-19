/*
 * Copyright 2026 Specter Ops, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/itup.h"
#include "access/relscan.h"
#include "access/stratnum.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/visibilitymap.h"
#include "catalog/namespace.h"
#include "catalog/partition.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_class.h"
#include "catalog/pg_index.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "partitioning/partbounds.h"
#include "partitioning/partdesc.h"
#include "storage/bufmgr.h"
#include "utils/array.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/partcache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(p5_native_adjacency_scan_v1);

#define P5_NATIVE_ROW_CAP 4096

typedef struct P5NativeArrays
{
	ArrayBuildState *edgeIDs;
	ArrayBuildState *nextNodeIDs;
	ArrayBuildState *kindIDs;
} P5NativeArrays;

static Oid p5_native_edge_partition_oid(int32 graphID);
static bool p5_native_edge_parent_matches(Relation edgeParent);
static Oid p5_native_covering_index_oid(Relation edgeRelation, bool inbound);
static bool p5_native_index_matches(Relation indexRelation, AttrNumber anchorAttribute,
									AttrNumber nextAttribute);
static bool p5_native_kind_matches(int16 kindID, Datum *kindIDs, int kindCount);
static void p5_native_append(P5NativeArrays *arrays, int64 edgeID, int64 nextNodeID,
						 int16 kindID);

Datum
p5_native_adjacency_scan_v1(PG_FUNCTION_ARGS)
{
	const int32 graphID = PG_GETARG_INT32(0);
	const int64 anchorID = PG_GETARG_INT64(1);
	ArrayType *kindFilter = PG_GETARG_ARRAYTYPE_P(2);
	const bool inbound = PG_GETARG_BOOL(3);
	Datum *requestedKinds = NULL;
	bool *kindNulls = NULL;
	int requestedKindCount = 0;
	Oid edgePartitionOID;
	Oid indexOID;
	Relation edgeRelation = NULL;
	Relation indexRelation = NULL;
	IndexScanDesc scan = NULL;
	TupleTableSlot *slot = NULL;
	Buffer visibilityMapBuffer = InvalidBuffer;
	ScanKeyData scanKey;
	P5NativeArrays arrays = {
		.edgeIDs = initArrayResult(INT8OID, CurrentMemoryContext, false),
		.nextNodeIDs = initArrayResult(INT8OID, CurrentMemoryContext, false),
		.kindIDs = initArrayResult(INT2OID, CurrentMemoryContext, false),
	};
	int64 scannedIndexTuples = 0;
	int64 heapFetches = 0;
	int returnedRows = 0;
	bool overflow = false;
	Datum values[8];
	bool nulls[8] = {false, false, false, false, false, false, false, false};
	ReturnSetInfo *resultInfo = (ReturnSetInfo *) fcinfo->resultinfo;

	if (!IsA(resultInfo, ReturnSetInfo) ||
		!(resultInfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("p5 native adjacency probe requires materialize mode")));

	if (ARR_NDIM(kindFilter) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("edge_kind_ids must be one-dimensional")));

	if (array_contains_nulls(kindFilter))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("edge_kind_ids must not contain nulls")));

	deconstruct_array_builtin(kindFilter, INT2OID, &requestedKinds, &kindNulls,
						  &requestedKindCount);

	if (!ActiveSnapshotSet())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("p5 native adjacency probe requires an active snapshot")));

	edgePartitionOID = p5_native_edge_partition_oid(graphID);
	edgeRelation = table_open(edgePartitionOID, AccessShareLock);
	indexOID = p5_native_covering_index_oid(edgeRelation, inbound);
	indexRelation = index_open(indexOID, AccessShareLock);

	ScanKeyInit(&scanKey,
				1,
				BTEqualStrategyNumber,
				F_INT8EQ,
				Int64GetDatum(anchorID));
	scan = index_beginscan(edgeRelation, indexRelation, GetActiveSnapshot(), NULL, 1, 0);
	scan->xs_want_itup = true;
	index_rescan(scan, &scanKey, 1, NULL, 0);
	slot = table_slot_create(edgeRelation, NULL);

	while (index_getnext_tid(scan, ForwardScanDirection) != NULL)
	{
		IndexTuple indexTuple;
		bool isNull = false;
		int16 kindID;
		int64 edgeID;
		int64 nextNodeID;

		CHECK_FOR_INTERRUPTS();
		scannedIndexTuples++;

		indexTuple = scan->xs_itup;
		if (indexTuple == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("validated covering index did not return an index tuple")));

		kindID = DatumGetInt16(index_getattr(indexTuple, 2, scan->xs_itupdesc, &isNull));
		if (isNull)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("validated covering index returned a null kind ID")));

		if (!p5_native_kind_matches(kindID, requestedKinds, requestedKindCount))
			continue;

		if (!VM_ALL_VISIBLE(edgeRelation,
							ItemPointerGetBlockNumber(&scan->xs_heaptid),
							&visibilityMapBuffer))
		{
			heapFetches++;
			if (!index_fetch_heap(scan, slot))
				continue;
		}

		edgeID = DatumGetInt64(index_getattr(indexTuple, 3, scan->xs_itupdesc, &isNull));
		if (isNull)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("validated covering index returned a null edge ID")));

		nextNodeID = DatumGetInt64(index_getattr(indexTuple, 4, scan->xs_itupdesc, &isNull));
		if (isNull)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("validated covering index returned a null next node ID")));

		if (returnedRows == P5_NATIVE_ROW_CAP)
		{
			overflow = true;
			break;
		}

		p5_native_append(&arrays, edgeID, nextNodeID, kindID);
		returnedRows++;
	}

	if (visibilityMapBuffer != InvalidBuffer)
		ReleaseBuffer(visibilityMapBuffer);
	ExecDropSingleTupleTableSlot(slot);
	index_endscan(scan);
	index_close(indexRelation, AccessShareLock);
	table_close(edgeRelation, AccessShareLock);

	InitMaterializedSRF(fcinfo, 0);
	values[0] = makeArrayResult(arrays.edgeIDs, CurrentMemoryContext);
	values[1] = makeArrayResult(arrays.nextNodeIDs, CurrentMemoryContext);
	values[2] = makeArrayResult(arrays.kindIDs, CurrentMemoryContext);
	values[3] = Int64GetDatum(scannedIndexTuples);
	values[4] = Int64GetDatum(heapFetches);
	values[5] = Int32GetDatum(returnedRows);
	values[6] = BoolGetDatum(overflow);
	values[7] = BoolGetDatum(!overflow);
	tuplestore_putvalues(resultInfo->setResult, resultInfo->setDesc, values, nulls);

	PG_RETURN_NULL();
}

static Oid
p5_native_edge_partition_oid(int32 graphID)
{
	Oid publicNamespace = get_namespace_oid("public", false);
	Oid edgeParentOID = get_relname_relid("edge", publicNamespace);
	char partitionName[NAMEDATALEN];
	Oid partitionOID;
	Relation edgeParent;
	PartitionDesc partitionDescription;
	PartitionBoundInfo boundInfo;
	int datumIndex;
	bool exactBound = false;

	if (!OidIsValid(edgeParentOID))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("required public.edge parent relation does not exist")));

	edgeParent = table_open(edgeParentOID, AccessShareLock);
	if (!p5_native_edge_parent_matches(edgeParent))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("public.edge does not have the required P5 native scan layout")));

	snprintf(partitionName, sizeof(partitionName), "edge_%d", graphID);
	partitionOID = get_relname_relid(partitionName, publicNamespace);
	if (!OidIsValid(partitionOID) ||
		get_partition_parent(partitionOID, false) != edgeParentOID)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("graph %d does not have the required public.edge partition", graphID)));

	partitionDescription = RelationGetPartitionDesc(edgeParent, true);
	boundInfo = partitionDescription->boundinfo;
	for (datumIndex = 0; datumIndex < boundInfo->ndatums; datumIndex++)
	{
		int partitionIndex;

		if (DatumGetInt32(boundInfo->datums[datumIndex][0]) != graphID)
			continue;

		partitionIndex = boundInfo->indexes[datumIndex];
		exactBound = partitionIndex >= 0 &&
			partitionDescription->oids[partitionIndex] == partitionOID;
		break;
	}

	table_close(edgeParent, AccessShareLock);
	if (!exactBound)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("public.edge partition bound does not match graph %d", graphID)));

	return partitionOID;
}

static bool
p5_native_edge_parent_matches(Relation edgeParent)
{
	PartitionKey partitionKey = RelationGetPartitionKey(edgeParent);
	TupleDesc description = RelationGetDescr(edgeParent);

	return partitionKey != NULL &&
		partitionKey->strategy == PARTITION_STRATEGY_LIST &&
		partitionKey->partnatts == 1 && partitionKey->partattrs[0] == 2 &&
		description->natts >= 5 &&
		TupleDescAttr(description, 0)->atttypid == INT8OID &&
		TupleDescAttr(description, 1)->atttypid == INT4OID &&
		TupleDescAttr(description, 2)->atttypid == INT8OID &&
		TupleDescAttr(description, 3)->atttypid == INT8OID &&
		TupleDescAttr(description, 4)->atttypid == INT2OID;
}

static Oid
p5_native_covering_index_oid(Relation edgeRelation, bool inbound)
{
	const AttrNumber anchorAttribute = inbound ? 4 : 3;
	const AttrNumber nextAttribute = inbound ? 3 : 4;
	Oid edgeParentOID = get_partition_parent(RelationGetRelid(edgeRelation), false);
	Relation edgeParent;
	List *indexList;
	ListCell *cell;

	if (!OidIsValid(edgeParentOID))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("graph edge partition is not attached to public.edge")));

	edgeParent = table_open(edgeParentOID, AccessShareLock);
	indexList = RelationGetIndexList(edgeParent);
	foreach(cell, indexList)
	{
		Oid parentIndexOID = lfirst_oid(cell);
		Oid childIndexOID;
		Relation parentIndexRelation = index_open(parentIndexOID, AccessShareLock);
		bool matches = parentIndexRelation->rd_rel->relkind == RELKIND_PARTITIONED_INDEX &&
			p5_native_index_matches(parentIndexRelation, anchorAttribute, nextAttribute);

		index_close(parentIndexRelation, AccessShareLock);
		if (!matches)
			continue;

		childIndexOID = index_get_partition(edgeRelation, parentIndexOID);
		if (OidIsValid(childIndexOID))
		{
			Relation childIndexRelation = index_open(childIndexOID, AccessShareLock);

			matches = p5_native_index_matches(childIndexRelation, anchorAttribute, nextAttribute);
			index_close(childIndexRelation, AccessShareLock);
			if (matches)
			{
				table_close(edgeParent, AccessShareLock);
				return childIndexOID;
			}
		}
	}
	table_close(edgeParent, AccessShareLock);

	ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_OBJECT),
			 errmsg("graph edge partition does not have the required %s covering index",
					inbound ? "inbound" : "outbound")));

	pg_unreachable();
}

static bool
p5_native_index_matches(Relation indexRelation, AttrNumber anchorAttribute,
						AttrNumber nextAttribute)
{
	Form_pg_index indexForm = indexRelation->rd_index;

	return indexRelation->rd_rel->relam == BTREE_AM_OID &&
		indexForm->indisvalid && indexForm->indisready && indexForm->indislive &&
		indexForm->indnkeyatts == 2 && indexForm->indnatts == 4 &&
		indexForm->indkey.values[0] == anchorAttribute &&
		indexForm->indkey.values[1] == 5 &&
		indexForm->indkey.values[2] == 1 &&
		indexForm->indkey.values[3] == nextAttribute;
}

static bool
p5_native_kind_matches(int16 kindID, Datum *kindIDs, int kindCount)
{
	int index;

	for (index = 0; index < kindCount; index++)
	{
		if (kindID == DatumGetInt16(kindIDs[index]))
			return true;
	}

	return kindCount == 0;
}

static void
p5_native_append(P5NativeArrays *arrays, int64 edgeID, int64 nextNodeID, int16 kindID)
{
	arrays->edgeIDs = accumArrayResult(arrays->edgeIDs, Int64GetDatum(edgeID), false,
								  INT8OID, CurrentMemoryContext);
	arrays->nextNodeIDs = accumArrayResult(arrays->nextNodeIDs, Int64GetDatum(nextNodeID), false,
									  INT8OID, CurrentMemoryContext);
	arrays->kindIDs = accumArrayResult(arrays->kindIDs, Int16GetDatum(kindID), false,
								   INT2OID, CurrentMemoryContext);
}
