package schema

import "strings"

type DbPath struct {
	Path   string
	Folder string
}

func (p DbPath) String() string {
	w := new(strings.Builder)
	w.Grow(len(p.Path) + len(p.Folder) + 1)
	w.WriteString(p.Path)
	if p.Folder != "" {
		w.WriteString("/")
		w.WriteString(p.Folder)
	}
	return w.String()
}

// FullTable returns full table name
func (p DbPath) FullTable(name string) string {
	w := new(strings.Builder)
	w.Grow(len(p.Path) + len(p.Folder) + len(name) + 2)
	w.WriteString(p.Path)
	if p.Folder != "" {
		w.WriteString("/")
		w.WriteString(p.Folder)
	}
	w.WriteString("/")
	w.WriteString(name)
	return w.String()
}

func (p DbPath) Table(name string) string {
	w := new(strings.Builder)
	if p.Folder != "" {
		w.WriteString(p.Folder)
		w.WriteString("/")
	}
	w.WriteString(name)
	return w.String()
}
