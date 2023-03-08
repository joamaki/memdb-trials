package state

import "net/netip"

type Meta struct {
	ID string

	Name      string
	Namespace string
	Labels    map[string]string
}

type MetaGetter interface {
	GetName() string
	GetNamespace() string
	GetLabels() map[string]string
}

func (m *Meta) GetName() string              { return m.Name }
func (m *Meta) GetNamespace() string         { return m.Namespace }
func (m *Meta) GetLabels() map[string]string { return m.Labels }

type Identity struct {
	Meta
}

type Node struct {
	Meta

	// TODO: Build indexing for sub-structs and then add NodeSpec?
	Identity uint64
	Address  netip.Addr
	Status   string
}
