// Read pulp generated metadata files, and keep an in-memory image of the registry
package pulp

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
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
	Type      string `json:"type"`
	// Arrays of tags and digests the image manifests reference
	Schema2Data []string `json:"schema2_data"`
	// Array of tags and digests that manifest lists reference
	ManifestListData []string `json:"manifest_list_data"`
	// Map of tag-> [digest,version]
	Amd64Tags map[string][]interface{}
}

// Contains the file information, and the repoid described in this file
type fileMapping struct {
	info   os.FileInfo
	repoId string
}

type pulpMetadata struct {
	fs memFS
	// File name -> FuleMapping map. This is used to detect changes
	files map[string]fileMapping
	// Repo name -> repoMd map.
	repos map[string]repoMd
	mu    sync.Mutex
}

var (
	pulpMd   *pulpMetadata
	updating bool
	updateMu sync.Mutex
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

func (md *pulpMetadata) scanDir(dir string) (bool, error) {
	finfo, err := ioutil.ReadDir(dir)
	if err != nil {
		return false, err
	}

	var wg sync.WaitGroup

	changed := false
	// For all files in dir, check if they've been modified
	for _, f := range finfo {
		info, ok := md.files[f.Name()]
		fileModified := false
		if ok {
			if !info.info.ModTime().Equal(f.ModTime()) {
				fileModified = true
			}
		} else {
			fileModified = true
		}
		rootDir := md.fs.Mkdir(registryRoot + "repositories")
		if fileModified {
			changed = true
			var rmd repoMd
			_, err := readJsonFile(path.Join(dir, f.Name()), &rmd)
			if err == nil {
				parts := strings.Split(rmd.RepoId, "/")
				if parts[0] == "library" {
					parts = parts[1:]
					rmd.RepoId = strings.Join(parts, "/")
				}
				// Remove the repo first
				rootDir.Delete(parts[0])
				fmt.Printf("Loading %s\n", f.Name())
				if rmd.Version == 2 {
					md.repos[rmd.RepoId] = rmd
					md.files[f.Name()] = fileMapping{info: f, repoId: rmd.RepoId}
					md.processV2Data(&wg, &rmd)
				} else if rmd.Version == 4 || rmd.Version == 3 {
					md.repos[rmd.RepoId] = rmd
					md.files[f.Name()] = fileMapping{info: f, repoId: rmd.RepoId}
					md.processV2Data(&wg, &rmd)
				}
			}
		}
	}
	wg.Wait()

	// Remove repos that no longer exist
	for name, f := range md.files {
		found := false
		for _, info := range finfo {
			if info.Name() == name {
				found = true
				break
			}
		}
		if !found {
			// file no longer exists
			delete(md.repos, f.repoId)
			delete(md.files, name)
		}
	}

	return changed, nil
}

// getStr gets a string from the map
func getStr(m map[string]interface{}, key string) (string, bool) {
	if v, ok := m[key]; ok {
		return fmt.Sprint(v), true
	}
	return "", false
}

// getInt gets an int from the map
func getInt(m map[string]interface{}, key string) (int, bool) {
	if v, ok := m[key]; ok {
		i, err := strconv.Atoi(fmt.Sprint(v))
		if err != nil {
			return 0, false
		}
		return i, true
	}
	return 0, false
}

func (md *pulpMetadata) processV2Data(wg *sync.WaitGroup, rmd *repoMd) {
	// Get image data
	// Get tags
	tags, _, err := httpGetContent(joinUrl(rmd.Url, "tags", "list"))
	if err == nil {
		var tagData map[string]interface{}
		if json.Unmarshal(tags, &tagData) == nil {
			itags, ok := tagData["tags"]
			if ok {
				for _, tag := range itags.([]interface{}) {
					stag := tag.(string)
					fmt.Printf("%s:%s\n", rmd.RepoId, tag)
					wg.Add(1)
					go func() {
						defer wg.Done()
						md.processManifest(rmd.RepoId, rmd.Url, stag)
					}()
				}
			}
		}
	} else {
		fmt.Printf("Cannot read %s: %s\n", rmd.RepoId, err.Error())
	}
}

func (md *pulpMetadata) pushV1ManifestData(repoId, url, tag string,
	manifestDigest digest.Digest,
	manifestData map[string]interface{}) {
	fsLayers, ok := manifestData["fsLayers"].([]interface{})
	if ok {
		layers := make([]digest.Digest, 0)
		for _, layer := range fsLayers {
			ilayer, ok := layer.(map[string]interface{})
			if ok {
				d, _ := digest.Parse(ilayer["blobSum"].(string))
				layers = append(layers, d)
			}
		}
		md.pushImage(repoId, tag, url, manifestDigest, layers)
	} else {
		fmt.Printf("Invalid manifest %s/%s:%s\n", repoId, tag, url)
	}
}

func (md *pulpMetadata) pushV2ManifestData(repoId, url, tag string,
	manifestDigest digest.Digest,
	manifestData map[string]interface{}) {
	ilayers, ok := manifestData["layers"].([]interface{})
	if ok {
		layers := make([]digest.Digest, 0)
		for _, ilayer := range ilayers {
			layer, ok := ilayer.(map[string]interface{})
			if ok {
				dg, ok := layer["digest"]
				if ok {
					mdigest, _ := digest.Parse(fmt.Sprint(dg))
					layers = append(layers, mdigest)
				}
			}
		}
		md.pushImage(repoId, tag, url, manifestDigest, layers)
	} else {
		fmt.Printf("Invalid manifest %s/%s:%s\n", repoId, tag, url)
	}
}

func (md *pulpMetadata) pushManifestList(repoId, url, tag string,
	manifestDigest digest.Digest,
	manifestData map[string]interface{}) {
	manifests, ok := manifestData["manifests"]
	if ok {
		// push manifest list data
		md.pushRevision(repoId, tag, manifestDigest)
		md.pushTag(repoId, tag, manifestDigest)
		md.pushManifestBlob(tag, url, manifestDigest)

		if manifestArr, ok := manifests.([]interface{}); ok {
			for _, iplatformManifest := range manifestArr {
				if platformManifest, ok := iplatformManifest.(map[string]interface{}); ok {
					idigest, ok := platformManifest["digest"]
					if ok {
						if _, ok := getStr(platformManifest, "mediaType"); ok {
							dg, _ := digest.Parse(fmt.Sprint(idigest))
							manifestData, _, err := httpGetContent(joinUrl(url, "blobs", dg.Algorithm().String(),
								dg.Hex()[0:2], dg.Hex()))
							if err != nil {
								md.processManifestData(repoId, url, tag, manifestData)
							} else {
								fmt.Printf("Cannot read manifest for %s", dg)
							}
						}
					}
				}
			}
		}
	} else {
		fmt.Printf("Cannot get manifests %s/%s:%s", repoId, tag, url)
	}
}

func (md *pulpMetadata) processManifest(repoId, url, tag string) {
	// Get the manifest
	fmt.Printf("retrieving %s\n", joinUrl(url, "manifests", tag))
	manifest, _, err := httpGetContent(joinUrl(url, "manifests", tag))
	if err == nil {
		md.processManifestData(repoId, url, tag, manifest)
	} else {
		fmt.Printf("Cannot retrieve %s\n", joinUrl(url, "manifests", tag))
	}
}

func (md *pulpMetadata) processManifestData(repoId, url, tag string, manifest []byte) {
	var manifestData map[string]interface{}
	if err := json.Unmarshal(manifest, &manifestData); err == nil {
		manifestDigest := digest.FromBytes(manifest)
		schemaVersion, ok := getInt(manifestData, "schemaVersion")
		if !ok {
			fmt.Printf("Cannot get schema version from manifest: %v", manifestData)
		}
		switch schemaVersion {
		case 1:
			// push the image
			md.pushV1ManifestData(repoId, url, tag, manifestDigest, manifestData)

		case 2:
			if mediaType, ok := getStr(manifestData, "mediaType"); ok {
				if mediaType == "application/vnd.docker.distribution.manifest.list.v2+json" {
					// Manifest list
					md.pushManifestList(repoId, url, tag, manifestDigest, manifestData)
				} else {
					// Manifest
					md.pushV2ManifestData(repoId, url, tag, manifestDigest, manifestData)
				}
			} else {
				fmt.Printf("Cannot get mediaType %s/%s:%s\n", repoId, tag, url)
			}
		}
	} else {
		fmt.Printf("Cannot parse manifest: %s\n", err.Error())
	}
}

func (md *pulpMetadata) pushImage(name, tag, url string, manifestDigest digest.Digest, layerDigests []digest.Digest) {
	fmt.Printf("pushImage %s %s %s\n", name, tag, url)
	md.pushLayers(name, layerDigests)
	md.pushRevision(name, tag, manifestDigest)
	md.pushTag(name, tag, manifestDigest)
	md.pushManifestBlob(tag, url, manifestDigest)
	for _, l := range layerDigests {
		md.pushLayerBlob(url, l)
	}
}

func (md *pulpMetadata) pushLayers(name string, layerDigests []digest.Digest) {
	imageDir := md.fs.Mkdir(registryRoot + "repositories/" + name)
	layers := imageDir.Mkdir("_layers")

	// push layers
	for _, layer := range layerDigests {
		datadir := layers.Mkdir(string(layer.Algorithm())).Mkdir(layer.Hex())
		datadir.Create("link", []byte(layer.String()), "")
	}

}

func (md *pulpMetadata) pushRevision(name, tag string, manifestDigest digest.Digest) {
	imageDir := md.fs.Mkdir(registryRoot + "repositories/" + name)
	manifests := imageDir.Mkdir("_manifests")

	revisions := manifests.Mkdir("revisions")
	dataDir := revisions.Mkdir(string(manifestDigest.Algorithm())).Mkdir(manifestDigest.Hex())
	dataDir.Create("link", []byte(manifestDigest.String()), "")
}

func (md *pulpMetadata) pushTag(name, tag string, manifestDigest digest.Digest) {
	imageDir := md.fs.Mkdir(registryRoot + "repositories/" + name)
	manifests := imageDir.Mkdir("_manifests")

	tags := manifests.Mkdir("tags")
	tagDir := tags.Mkdir(tag)
	current := tagDir.Mkdir("current")
	current.Create("link", []byte(manifestDigest.String()), "")

	index := tagDir.Mkdir("index")
	datadir := index.Mkdir(string(manifestDigest.Algorithm())).Mkdir(manifestDigest.Hex())
	datadir.Create("link", []byte(manifestDigest.String()), "")
}

func (md *pulpMetadata) pushManifestBlob(tag, url string, manifestDigest digest.Digest) {
	md.pushBlob(manifestDigest, joinUrl(url, "manifests", tag), false)
}

func (md *pulpMetadata) pushLayerBlob(url string, layerDigest digest.Digest) {
	md.pushBlob(layerDigest, joinUrl(url, "blobs", layerDigest.String()), true)
}

func (md *pulpMetadata) pushBlob(d digest.Digest, url string, layer bool) {
	dir := md.fs.Mkdir(registryRoot + "blobs")
	alg := dir.Mkdir(d.Algorithm().String())
	twodigs := alg.Mkdir(d.Hex()[0:2])
	datadir := twodigs.Mkdir(d.Hex())
	datadir.Create("data", nil, url).isLayer = layer
}

func (md *pulpMetadata) isManifestRequest(blobPath string) (manifestReq bool, name string, tag string) {
	// Find blobs in the path
	parts := strings.Split(blobPath, "/")
	for i, part := range parts {
		if part == "blobs" {
			// now we have algorithm, 2 digits, and digest
			if i+4 <= len(parts) {
				digest := digest.NewDigestFromHex(parts[i+1], parts[i+3])
				return md.isManifestDigest(digest)
			}
		}
	}
	return false, "", ""
}

func (md *pulpMetadata) isManifestDigest(d digest.Digest) (manifestReq bool, name string, tag string) {
	r := md.fs.Find("/docker/registry/v2/repositories")
	if r != nil {
		root := r.(*mdirectory)
		var foundDir *mdirectory
		if !root.Walk(func(dir *mdirectory) bool {
			if dir.parent.parent.name == "index" &&
				dir.parent.parent.parent.parent.name == "tags" &&
				dir.parent.name == d.Algorithm().String() &&
				dir.name == d.Hex() {
				foundDir = dir
				return false
			}
			return true
		}) {
			tagDir := foundDir.parent.parent.parent
			tag = tagDir.name
			manifestDir := tagDir.parent.parent
			name = ""
			for x := manifestDir.parent; x.name != "repositories"; x = x.parent {
				if len(name) == 0 {
					name = x.name
				} else {
					name = x.name + "/" + name
				}
			}
			return true, name, tag
		}
	}
	return false, "", ""
}

func updateMd(dir string) {
	if !updating {
		updateMu.Lock()
		updating = true
		fmt.Printf("UpdateMd %s\n", dir)
		if pulpMd == nil {
			pulpMd = &pulpMetadata{}
			pulpMd.files = make(map[string]fileMapping)
			pulpMd.repos = make(map[string]repoMd)
			pulpMd.fs = newFS()
		}
		changed, _ := pulpMd.scanDir(dir)
		if changed {
			fmt.Printf("%s\n", pulpMd.fs.PrintFiles())
		}
		updating = false
		updateMu.Unlock()
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
