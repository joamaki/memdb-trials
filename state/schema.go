package state

import (
	"fmt"

	memdb "github.com/hashicorp/go-memdb"
)

type Index string

const (
	identityTable = "identities"
	nodeTable     = "nodes"
)

const (
	NameIndex      = Index("name")
	NamespaceIndex = Index("namespace")
	IDIndex        = Index("id")
	IdentityIndex  = Index("identity")
)

var allTables = []func() *memdb.TableSchema{
	identityTableSchema,
	nodeTableSchema,
}

func schema() *memdb.DBSchema {
	dbSchema := &memdb.DBSchema{
		Tables: make(map[string]*memdb.TableSchema),
	}
	for _, sfn := range allTables {
		s := sfn()
		dbSchema.Tables[s.Name] = s
	}
	return dbSchema
}

func identityTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: identityTable,
		Indexes: map[string]*memdb.IndexSchema{
			"id":              idIndexSchema,
			string(NameIndex): nameIndexSchema,
		},
	}
}

func nodeTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: nodeTable,
		Indexes: map[string]*memdb.IndexSchema{
			string(IDIndex):        idIndexSchema,
			string(NamespaceIndex): namespaceIndexSchema,
			string(NameIndex):      nameIndexSchema,
			string(IdentityIndex): {
				Name:         string(IdentityIndex),
				AllowMissing: false,
				Unique:       true,
				Indexer:      &memdb.UintFieldIndex{Field: "Identity"},
			},
		},
	}
}

var idIndexSchema = &memdb.IndexSchema{
	Name:         "id",
	AllowMissing: false,
	Unique:       true,
	Indexer:      &memdb.UUIDFieldIndex{Field: "ID"},
}

var namespaceIndexSchema = &memdb.IndexSchema{
	Name:         string(NamespaceIndex),
	AllowMissing: true,
	Unique:       false,
	Indexer:      &memdb.StringFieldIndex{Field: "Namespace"},
}

var nameIndexSchema = &memdb.IndexSchema{
	Name:         string(NameIndex),
	AllowMissing: false,
	Unique:       true,
	Indexer:      nameIndexer{},
}

// nameIndexer implements <namespace>/<name> indexing for all objects
// that implement MetaGetter or embed Meta.
type nameIndexer struct{}

func (nameIndexer) FromObject(obj interface{}) (bool, []byte, error) {
	meta, ok := obj.(MetaGetter)
	if !ok {
		return false, nil,
			fmt.Errorf("object %T does not implement MetaGetter", obj)
	}

	idx := meta.GetNamespace() + "/" + meta.GetName() + "\x00"
	return true, []byte(idx), nil
}

func (nameIndexer) FromArgs(args ...interface{}) ([]byte, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("must provide only a single argument")
	}
	arg, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("argument must be a string: %#v", args[0])
	}
	arg += "\x00"
	return []byte(arg), nil
}

func (m nameIndexer) PrefixFromArgs(args ...interface{}) ([]byte, error) {
	val, err := m.FromArgs(args...)
	if err != nil {
		return nil, err
	}

	// Strip the null terminator, the rest is a prefix
	n := len(val)
	if n > 0 {
		return val[:n-1], nil
	}
	return val, nil
}

type Query struct {
	Index Index
	Args  []any
}

func ByName(namespace string, name string) Query {
	return Query{NameIndex, []any{namespace + "/" + name}}
}

func ByNamespace(namespace string) Query {
	return Query{NamespaceIndex, []any{namespace}}
}

func ByID(id string) Query {
	return Query{IDIndex, []any{id}}
}

func ByIdentity(id uint64) Query {
	return Query{IdentityIndex, []any{id}}
}
