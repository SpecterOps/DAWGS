package vendormocks

//go:generate go run go.uber.org/mock/mockgen -copyright_file=../../../../LICENSE.header -destination=./neo4j/neo4j-go-driver/v5/neo4j/mock.go -package=neo4j github.com/neo4j/neo4j-go-driver/v5/neo4j Result,Transaction,Session
//go:generate go run go.uber.org/mock/mockgen -copyright_file=../../../../LICENSE.header -destination=./jackc/pgx/v5/mock.go -package=pgx github.com/jackc/pgx/v5 Tx
