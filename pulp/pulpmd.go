// Read pulp generated metadata files, and keep an in-memory image of the registry
package pulp

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"time"
)

// Basic repo metadata structure expected to be read from metadata files (v2)
type repoMd struct {
	RepoId    string `json:"repo-registry-id"`
	Url       string `json:"url"`
	Version   int    `json:"version"`
	Protected bool   `json:"protected"`
}

// fileData keeps Information about a file and its contents
type fileData struct {
	repoMd
	fileInfo os.FileInfo
}

// pulpData maps file names to file data and repo ids to file data
type pulpData struct {
	files map[string]*fileData
	repos map[string]*fileData
}

var (
	dirContents *pulpData
	dirMu       sync.Mutex
)

func newPulpData() *pulpData {
	return &pulpData{files: make(map[string]*fileData),
		repos: make(map[string]*fileData)}
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
	pd, err := readFilesInDir(dir, dirContents, defaultModifiedFunc)
	if err != nil {
	} else {
		dirMu.Lock()
		dirContents = pd
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
