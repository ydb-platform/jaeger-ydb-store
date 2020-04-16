package schema

import (
	"path"
)

type DbPath struct {
	Path   string
	Folder string
}

func (p DbPath) String() string {
	return path.Join(p.Path, p.Folder)
}

// FullTable returns full table name
func (p DbPath) FullTable(name string) string {
	return path.Join(p.Path, p.Folder, name)
}

func (p DbPath) Table(name string) string {
	return path.Join(p.Folder, name)
}
