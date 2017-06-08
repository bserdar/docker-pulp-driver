package pulp

import (
	"path"
	"strings"
	"sync"
	"time"
)

// Minimaal in-memory file system thar models a docker registry directory structure
type memFS struct {
	root *mdirectory
}

type node interface {
	getName() string
	isDir() bool
	getModTime() time.Time
	getFullPath() string
}

type mdirectory struct {
	mu sync.RWMutex
	// directory name
	name string
	// parent directory. Roor points to itself
	parent *mdirectory
	// Modification time
	modTime time.Time
	// Child nodes--files and dirs
	nodes map[string]node
}

func (m *mdirectory) getName() string       { return m.name }
func (m *mdirectory) isDir() bool           { return true }
func (m *mdirectory) getModTime() time.Time { return m.modTime }
func (m *mdirectory) getFullPath() string   { return m.FullPath() }

type mfile struct {
	// directory containing the file
	dir *mdirectory
	// name of the file
	name string
	// Size of the file
	size int64
	// modification time
	modTime time.Time
	// file contents
	contents []byte
	// If the file contents are located somewhere else, this points to that
	redirectUrl string
	// Is the file content somewhere else?
	isRedirect bool

	// Is this a layer
	isLayer bool
}

func (m *mfile) getName() string       { return m.name }
func (m *mfile) isDir() bool           { return false }
func (m *mfile) getModTime() time.Time { return m.modTime }
func (m *mfile) getFullPath() string   { return m.FullPath() }

// newFS returns a new in-memory filesystem
func newFS() memFS {
	fs := memFS{root: &mdirectory{modTime: time.Now(),
		nodes: make(map[string]node)}}
	fs.root.parent = fs.root
	return fs
}

// splitPath splits the path to its components
func splitPath(path string) []string {
	parts := strings.Split(path, "/")
	// If path is of the form /d1/d2/..., then the first segment is empty
	if len(parts[0]) == 0 {
		parts = parts[1:]
	}
	// Remove the part after the trailing /
	if len(parts[len(parts)-1]) == 0 {
		parts = parts[0 : len(parts)-1]
	}
	return parts
}

// GetFiles returns all the files (recursively) under this directory
func (d *mdirectory) GetFiles() []*mfile {
	d.mu.RLock()
	defer d.mu.RUnlock()

	files := make([]*mfile, 0)
	d.Walk(func(dir *mdirectory) bool {
		for _, n := range dir.nodes {
			if !n.isDir() {
				files = append(files, n.(*mfile))
			}
		}
		return true
	})
	return files
}

// Mkdir creates a new directory under current dir
// Returns the new directory, or the existing directory. Panics in case of error
func (d *mdirectory) Mkdir(name string) *mdirectory {
	d.mu.Lock()
	defer d.mu.Unlock()

	f, ok := d.nodes[name]
	if ok {
		if f.isDir() {
			return f.(*mdirectory)
		} else {
			panic("A file already exists:" + name)
		}
	}
	newdir := mdirectory{name: name,
		parent:  d,
		nodes:   make(map[string]node),
		modTime: time.Now()}
	d.nodes[name] = &newdir
	return &newdir
}

func (d *mdirectory) Delete(name string) {
	d.mu.Lock()
	delete(d.nodes, name)
	d.mu.Unlock()
}

// Create creates a new file under the directory
// Returns the new file and error. IF the file already exists, returns that file and ErrFileExists
func (d *mdirectory) Create(name string, content []byte, redir string) *mfile {
	d.mu.Lock()
	defer d.mu.Unlock()

	f, ok := d.nodes[name]
	if ok {
		if f.isDir() {
			panic("A file already exists:" + name)
		} else {
			return f.(*mfile)
		}
	}
	newFile := mfile{name: name,
		dir:     d,
		modTime: time.Now()}
	if content != nil {
		newFile.contents = content
		newFile.size = int64(len(content))
	} else if len(redir) != 0 {
		newFile.redirectUrl = redir
		newFile.isRedirect = true
	}
	d.nodes[name] = &newFile
	return &newFile
}

// Walk recursively walks the directories, until the end or f returns
// false. Returns false if f returned false, or true if all dirs are
// traversed
func (d *mdirectory) Walk(f func(*mdirectory) bool) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()

	for _, n := range d.nodes {
		if n.isDir() {
			dir := n.(*mdirectory)
			if !f(dir) {
				return false
			}
			if !dir.Walk(f) {
				return false
			}
		}
	}
	return true
}

func (d *mdirectory) FullPath() string {
	if d.parent == d {
		return "/"
	} else {
		return path.Join(d.parent.FullPath(), d.name)
	}
}

func (f *mfile) FullPath() string {
	return f.dir.FullPath() + "/" + f.name
}

// Mkdir makes a full path, and returns the directory
func (fs *memFS) Mkdir(fullPath string) *mdirectory {
	parts := splitPath(fullPath)
	// Walk the tree
	var cwd *mdirectory

	cwd = fs.root
	for _, s := range parts {
		cwd = cwd.Mkdir(s)
	}
	return cwd
}

// Find finds a file or dir, or nil if not found
func (fs *memFS) Find(fullPath string) node {
	parts := splitPath(fullPath)
	var cwd *mdirectory
	var f *mfile

	cwd = fs.root
	for _, s := range parts {
		cwd.mu.RLock()
		x, ok := cwd.nodes[s]
		cwd.mu.RUnlock()
		if !ok {
			return nil
		}
		if x.isDir() {
			cwd = x.(*mdirectory)
		} else {
			f = x.(*mfile)
		}
	}
	if f != nil {
		return f
	} else {
		return cwd
	}
}

func (fs *memFS) GetFiles() []*mfile {
	return fs.root.GetFiles()
}

func (fs *memFS) PrintFiles() string {
	s := ""
	for _, f := range fs.GetFiles() {
		s = s + f.FullPath() + "\n"
	}
	return s
}
