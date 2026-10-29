package pg

import (
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/graph"
)

// hasMultipartWrites identifies writes that need sequential PostgreSQL snapshots:
// mutations before WITH, or a query part combining DELETE with other writes.
func hasMultipartWrites(query *cypher.RegularQuery) bool {
	if query == nil || query.SingleQuery == nil {
		return false
	}
	part := query.SingleQuery.SinglePartQuery
	if multipart := query.SingleQuery.MultiPartQuery; multipart != nil {
		for _, part := range multipart.Parts {
			if part != nil && len(part.UpdatingClauses) > 0 {
				return true
			}
		}
		part = multipart.SinglePartQuery
	}
	if part == nil {
		return false
	}
	hasDelete, hasWrite := false, false
	for _, expression := range part.UpdatingClauses {
		clause, ok := expression.(*cypher.UpdatingClause)
		if !ok || clause == nil {
			continue
		}
		switch clause.Clause.(type) {
		case *cypher.Delete:
			hasDelete = true
		case *cypher.Create, *cypher.Set, *cypher.Remove, *cypher.Merge:
			hasWrite = true
		}
	}
	return hasDelete && hasWrite
}

// queryMultipart keeps the entire Cypher write in the caller's transaction,
// while giving WITH and DELETE boundaries visibility of preceding mutations.
func (s *transaction) queryMultipart(query *cypher.RegularQuery, parameters map[string]any, graphID int32, profile SQLGenerationProfile) graph.Result {
	defer func() { s.recordSQLGenerationProfile(profile) }()
	s.invalidateTopologyRouteDecisions()
	if s.tx == nil {
		return graph.NewErrorResult(fmt.Errorf("multipart writes require an explicit PostgreSQL write transaction"))
	}
	started := time.Now()
	translation, err := translate.Translate(s.ctx, query, s.schemaManager, parameters, graphID)
	profile.Translate += time.Since(started)
	if err != nil {
		return graph.NewErrorResult(err)
	}
	s.multipartSequence++
	prefix := fmt.Sprintf("__dawgs_multipart_%d_", s.multipartSequence)
	started = time.Now()
	statements, err := translate.TranslatedStatements(translation, prefix)
	profile.Format += time.Since(started)
	if err != nil {
		return graph.NewErrorResult(err)
	}
	if len(statements) != len(translation.Stages) || len(statements) < 2 {
		return graph.NewErrorResult(fmt.Errorf("multipart write has no sequential execution plan"))
	}
	started = time.Now()
	defer func() { profile.Dispatch += time.Since(started) }()

	// Do not retain prepared statements containing temporary relation OIDs.
	mode := pgx.QueryExecModeExec
	if s.queryExecMode == pgx.QueryExecModeSimpleProtocol {
		mode = pgx.QueryExecModeSimpleProtocol
	}
	tables := make([]string, 0, len(statements)-1)
	for index, statement := range statements {
		bindings := make(map[string]any, len(translation.Parameters)+len(statement.Parameters))
		maps.Copy(bindings, translation.Parameters)
		maps.Copy(bindings, statement.Parameters)
		arguments := []any{mode}
		if index == len(statements)-1 {
			arguments = append(arguments, s.queryResultsFormat)
		}
		if len(bindings) > 0 {
			arguments = append(arguments, pgx.NamedArgs(bindings))
		}
		if index != len(statements)-1 {
			if _, err := s.driver().Exec(s.ctx, statement.Statement, arguments...); err != nil {
				s.stagedError = fmt.Errorf("execute multipart stage %d: %w", index+1, err)
				return graph.NewErrorResult(s.stagedError)
			}
			tables = append(tables, pgx.Identifier{"pg_temp", prefix + translation.Stages[index].Binding.String()}.Sanitize())
			continue
		}
		rows, err := s.driver().Query(s.ctx, statement.Statement, arguments...)
		if err != nil {
			s.stagedError = fmt.Errorf("execute final multipart stage: %w", err)
			return graph.NewErrorResult(s.stagedError)
		}
		return &stagedQueryResult{
			Result: &queryResult{ctx: s.ctx, rows: rows, kindMapper: s.schemaManager},
			owner:  s,
			tables: tables,
		}
	}
	return graph.NewErrorResult(fmt.Errorf("multipart write has no final statement"))
}

// stagedQueryResult owns the temporary relations until its row stream closes.
// ON COMMIT DROP additionally handles transaction rollback and cancellation.
type stagedQueryResult struct {
	graph.Result
	owner  *transaction
	tables []string
	done   bool
	err    error
}

func (s *stagedQueryResult) Next() bool {
	if s.done {
		return false
	}
	if s.Result.Next() {
		return true
	}
	s.Close()
	return false
}

func (s *stagedQueryResult) Scan(targets ...any) error {
	if err := s.Result.Scan(targets...); err != nil {
		s.err = err
		s.owner.stagedError = err
		return err
	}
	return nil
}

func (s *stagedQueryResult) Error() error {
	if s.err != nil {
		return s.err
	}
	return s.Result.Error()
}

func (s *stagedQueryResult) Close() {
	if s.done {
		return
	}
	s.done = true
	// Closing pgx rows drains the server response and exposes late errors.
	s.Result.Close()
	if err := s.Result.Error(); err != nil && s.err == nil {
		s.err = err
	}
	if s.err == nil && len(s.tables) > 0 {
		_, s.err = s.owner.driver().Exec(s.owner.ctx, "drop table "+strings.Join(s.tables, ", "))
	}
	if s.err != nil {
		s.owner.stagedError = s.err
	}
}
