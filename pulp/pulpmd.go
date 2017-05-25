// Read pulp generated metadata files, and keep an in-memory image of the registry
package pulp

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"sync"
	"time"
)

// Basic repo metadata structure expected to be read from metadata files (v2)
type repoMd struct {
	RepoId  string `json:"repo-registry-id"`
	Url     string `json:"url"`
	Version int    `json:"version"`
}

// fileData keeps Information about a file and its contents
type fileData struct {
	repoMd
	fileInfo os.FileInfo
}

// pulpData maps file names to file data
type pulpData map[string]*fileData

var (
	dirContents pulpData
	dirMu       sync.Mutex
)

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

// Reads all modified files in the directory and returns pulpdata
func readFilesInDir(dir string, existing pulpData) (pulpData, error) {
	finfo, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	pd := make(pulpData)
	for _, f := range finfo {
		upToDate := false
		if existing != nil {
			if existingData, ok := existing[f.Name()]; ok {
				if f.ModTime().Equal(existingData.fileInfo.ModTime()) {
					upToDate = true
					pd[f.Name()] = existingData
				}
			}
		}
		if !upToDate {
			fd := fileData{fileInfo: f}
			readJsonFile(dir+"/"+fd.fileInfo.Name(), &fd.repoMd)
			pd[f.Name()] = &fd
		}
	}
	return pd, nil
}

func updateMd(dir string) {
	pd, err := readFilesInDir(dir, dirContents)
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
