package state

import (
	"fmt"

	memdb "github.com/hashicorp/go-memdb"
)

type State struct {
	db *memdb.MemDB
	r  Reflector
}

func New() (s *State, err error) {
	s = &State{}
	s.db, err = memdb.NewMemDB(schema())
	return
}

func (s *State) SetReflector(r Reflector) {
	s.r = r
}

func (s *State) WriteTx() StateTx {
	tx := s.db.Txn(true)
	if s.r != nil {
		tx.TrackChanges()
	}
	return &stateTx{tx, s.r}
}

// TODO different interface for reads
func (s *State) ReadTx() StateTx {
	return &stateTx{s.db.Txn(false), nil}
}

func (s *State) Nodes() (Iterator[*Node], error) {
	txn := s.db.Txn(false)
	resIt, err := txn.Get(nodeTable, string(NameIndex))
	if err != nil {
		return nil, fmt.Errorf("node get failed: %w", err)
	}
	return iterator[*Node]{resIt}, nil
}

func (s *State) Identities() (Iterator[*Identity], error) {
	txn := s.db.Txn(false)
	resIt, err := txn.Get(identityTable, string(NameIndex))
	if err != nil {
		return nil, fmt.Errorf("identity get failed: %w", err)
	}
	return iterator[*Identity]{resIt}, nil
}

type Iterator[Obj any] interface {
	// Next returns the next object and true, or zero value and false if iteration
	// has finished.
	Next() (Obj, bool)

	// Invalidated returns a channel that is closed when the results
	// returned by the iterator have changed in the database.
	Invalidated() <-chan struct{}
}

type iterator[Obj any] struct {
	it memdb.ResultIterator
}

func (s iterator[Obj]) Next() (obj Obj, ok bool) {
	if v := s.it.Next(); v != nil {
		obj = v.(Obj)
		ok = true
	}
	return
}

func (s iterator[Obj]) Invalidated() <-chan struct{} {
	return s.it.WatchCh()
}

type StateTx interface {
	Nodes() TableTx[*Node]
	Identities() TableTx[*Identity]

	// Defer pushes function to be run after transaction is
	// committed.
	Defer(fn func())

	Abort()
	Commit() error
}

type stateTx struct {
	tx *memdb.Txn
	r  Reflector
}

var _ StateTx = &stateTx{}

func (stx *stateTx) Abort()          { stx.tx.Abort() }
func (stx *stateTx) Defer(fn func()) { stx.tx.Defer(fn) }

func (stx *stateTx) Commit() error {
	if stx.r != nil {
		if err := stx.r.ProcessChanges(stx.tx.Changes()); err != nil {
			return err
		}
	}
	stx.tx.Commit()
	return nil
}

func (stx *stateTx) Nodes() TableTx[*Node] {
	return &tableTx[*Node]{
		table: nodeTable,
		tx:    stx.tx,
	}
}

func (stx *stateTx) Identities() TableTx[*Identity] {
	return &tableTx[*Identity]{
		table: identityTable,
		tx:    stx.tx,
	}
}

type TableTx[Obj any] interface {
	First(Query) (Obj, error)
	Last(Query) (Obj, error)
	Get(Query) (Iterator[Obj], error)

	Insert(obj Obj) error
	Delete(obj Obj) error

	// TODO prefixed ops

}

type tableTx[Obj any] struct {
	table string
	tx    *memdb.Txn
}

func (t *tableTx[Obj]) Delete(obj Obj) error {
	return t.tx.Delete(t.table, obj)
}

func (t *tableTx[Obj]) First(q Query) (obj Obj, err error) {
	var v any
	v, err = t.tx.First(t.table, string(q.Index), q.Args...)
	if err == nil && v != nil {
		obj = v.(Obj)
	}
	// TODO not found error or zero value Obj is fine?
	return
}

func (t *tableTx[Obj]) Get(q Query) (Iterator[Obj], error) {
	it, err := t.tx.Get(t.table, string(q.Index), q.Args...)
	if err != nil {
		return nil, err
	}
	return iterator[Obj]{it}, nil

}

func (t *tableTx[Obj]) Insert(obj Obj) error {
	return t.tx.Insert(t.table, obj)
}

func (t *tableTx[Obj]) Last(q Query) (obj Obj, err error) {
	var v any
	v, err = t.tx.Last(t.table, string(q.Index), q.Args...)
	if err == nil && v != nil {
		obj = v.(Obj)
	}
	// TODO not found error or zero value Obj is fine?
	return
}

var _ TableTx[struct{}] = &tableTx[struct{}]{}
