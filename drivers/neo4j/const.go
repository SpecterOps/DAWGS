package neo4j

// TODO: Deprecate these

const (
	cypherDeleteNodeByID          = `match (n) where id(n) = $id detach delete n`
	cypherDeleteNodesByID         = `match (n) where id(n) in $id_list detach delete n`
	cypherDeleteRelationshipByID  = `match ()-[r]->() where id(r) = $id delete r`
	cypherDeleteRelationshipsByID = `unwind $p as rid match ()-[r]->() where id(r) = rid delete r`
	idParameterName               = "id"
	idListParameterName           = "id_list"
)
