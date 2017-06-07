// Read pulp generated metadata files, and keep an in-memory image of the registry
package pulp

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"time"

	digest "github.com/opencontainers/go-digest"
)

// Basic repo metadata structure expected to be read from metadata files (v2)
type repoMd struct {
	RepoId    string `json:"repo-registry-id"`
	Url       string `json:"url"`
	Version   int    `json:"version"`
	Protected bool   `json:"protected"`
}

// tagInfo keeps the file information for the tag manifest
type tagInfo struct {
	// tag denotes the image tag
	tag string
	// hash is the layer digest
	hash string
	// size is the layer size
	size int64
	// modTime is the layer modification time
	modTime time.Time
}

// fileData keeps Information about a file and its contents
type fileData struct {
	repoMd
	fileInfo os.FileInfo
	// Map of tag manifest hash to tag names
	tagHashMap map[string]tagInfo
	// Lock this to modify tagHashMap
	mu sync.Mutex
}

// pulpData maps file names to file data and repo ids to file data
type pulpData struct {
	// Map of file names to filedata
	files map[string]*fileData
	// Map of repo names to filedata
	repos map[string]*fileData
}

var (
	pulpMetadata *pulpData
	dirMu        sync.Mutex
)

func newPulpData() *pulpData {
	return &pulpData{files: make(map[string]*fileData),
		repos: make(map[string]*fileData)}
}

func (f *fileData) getTagInfoByHash(hash string) (tagInfo, bool) {
	f.mu.Lock()
	ti, ok := f.tagHashMap[hash]
	f.mu.Unlock()
	if ok {
		return ti, true
	} else {
		return tagInfo{}, false
	}
}

func (f *fileData) getTagByHash(hash string) (string, bool) {
	f.mu.Lock()
	s, ok := f.getTagInfoByHash(hash)
	f.mu.Unlock()
	if ok {
		return s.tag, ok
	}
	return "", false
}

func (f *fileData) getTagInfoByTag(tag string) (tagInfo, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, ti := range f.tagHashMap {
		if ti.tag == tag {
			return ti, true
		}
	}
	return tagInfo{}, false
}

func (f *fileData) getHashByTag(tag string) (string, bool) {
	f.mu.Lock()
	defer d.mu.Unlock()
	for h, t := range f.tagHashMap {
		if tag == t.tag {
			return h, true
		}
	}
	return "", false
}

func findFileDataByManifestHash(hash string) *fileData {
	for _, fd := range pulpMetadata.repos {
		if _, ok := fd.getTagInfoByHash(hash); ok {
			return fd
		}
	}
	return nil
}

func (f *fileData) addTag(tag string, manifest []byte, modTime time.Time) string {
	ti := tagInfo{tag: tag}
	ti.hash = digest.FromBytes(manifest).String()
	ti.size = int64(len(manifest))
	ti.modTime = modTime
	f.tagHashMap[ti.hash] = ti
	return ti.hash
}

func (pd *pulpData) add(fd *fileData) {
	pd.files[fd.fileInfo.Name()] = fd
	pd.repos[fd.RepoId] = fd
}

// readJsonFile reads the  contents of a JSON file and returns a repoMd instance
func readJsonFile(file string, result *repoMd) (*repoMd, error) {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(data, result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func readFileData(dir string, f os.FileInfo) *fileData {
	fd := fileData{fileInfo: f}
	readJsonFile(path.Join(dir, f.Name()), &fd.repoMd)
	if fd.Version == 2 {
		fd.tagHashMap = make(map[string]tagInfo)
		return &fd
	}
	return nil
}

func defaultModifiedFunc(old, new os.FileInfo) bool {
	return !old.ModTime().Equal(new.ModTime())
}

// Reads all modified files in the directory and returns pulpdata
// The modified function returns true if the file is modified
func readFilesInDir(dir string, existing *pulpData, modified func(old, new os.FileInfo) bool) (*pulpData, error) {
	finfo, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	pd := newPulpData()
	for _, f := range finfo {
		readFile := false
		if existing != nil {
			if ex, ok := existing.files[f.Name()]; ok {
				if modified(f, ex.fileInfo) {
					// File modified
					readFile = true
				} else {
					// file Unmodified
					pd.add(ex)
				}
			} else {
				// New file
				readFile = true
			}
		} else {
			// New file
			readFile = true
		}
		if readFile {
			fd := readFileData(dir, f)
			if fd != nil {
				pd.add(fd)
			}
		}
	}
	return pd, nil
}

func updateMd(dir string) {
	fmt.Printf("UpdateMd %s\n", dir)
	pd, err := readFilesInDir(dir, pulpMetadata, defaultModifiedFunc)
	if err != nil {
		fmt.Printf("Error:%s\n", err.Error())
	} else {
		dirMu.Lock()
		pulpMetadata = pd
		dirMu.Unlock()
	}
}

func startDirWatch(dir string, pollingIntervalSecs uint64) {
	tkr := time.NewTicker(time.Duration(pollingIntervalSecs) * time.Second)
	go func() {
		for _ = range tkr.C {
			updateMd(dir)
		}
	}()
}
