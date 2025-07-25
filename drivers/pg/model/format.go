package model

import (
	"strconv"
	"strings"

	"github.com/specterops/dawgs/graph"
)

const (
	NodeTable = "node"
	EdgeTable = "edge"
)

func partitionTableName(parent string, graphID int32) string {
	return parent + "_" + strconv.FormatInt(int64(graphID), 10)
}

func NodePartitionTableName(graphID int32) string {
	return partitionTableName(NodeTable, graphID)
}

func EdgePartitionTableName(graphID int32) string {
	return partitionTableName(EdgeTable, graphID)
}

func IndexName(table string, index graph.Index) string {
	stringBuilder := strings.Builder{}

	stringBuilder.WriteString(table)
	stringBuilder.WriteString("_")
	stringBuilder.WriteString(index.Field)
	stringBuilder.WriteString("_index")

	return stringBuilder.String()
}

func ConstraintName(table string, constraint graph.Constraint) string {
	stringBuilder := strings.Builder{}

	stringBuilder.WriteString(table)
	stringBuilder.WriteString("_")
	stringBuilder.WriteString(constraint.Field)
	stringBuilder.WriteString("_constraint")

	return stringBuilder.String()
}
