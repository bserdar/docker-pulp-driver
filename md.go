package pulp

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	rctx "github.com/docker/distribution/context"
	digest "github.com/opencontainers/go-digest"
)

const (
	ManifestV1MediaType   = "application/vnd.docker.distribution.manifest.v1+json"
	ManifestV2MediaType   = "application/vnd.docker.distribution.manifest.v2+json"
	ManifestListMediaType = "application/vnd.docker.distribution.manifest.list.v2+json"
	LayerMediaType        = "application/vnd.docker.image.rootfs.diff.tar.gzip"
)

// Basic repo metadata structure expected to be read from metadata files
type repoMetadata struct {
	Filename  string
	RepoId    string `json:"repo-registry-id"`
	Url       string `json:"url"`
	Version   int    `json:"version"`
	Protected bool   `json:"protected"`
	Type      string `json:"type"`
	// Arrays of tags and digests the image manifests reference
	Schema2Data []string `json:"schema2_data"`
	// Array of tags and digests that manifest lists reference
	ManifestListData []string `json:"manifest_list_data"`
	// Amd64Tags contain tags for key, and each element has digest for the first element, and schema version for second
	Amd64Tags map[string][]interface{} `json:"manifest_list_amd64_tags"`
}

type repository struct {
	Name   string
	RepoMd *repoMetadata
	Tags   map[string]*tagData
}

func (r repository) String() string {
	return fmt.Sprintf("Repo name=%s, tags: %v", r.Name, r.Tags)
}

type tagData struct {
	// Map of digest -> Media type
	Digests map[string]string
}

func (t tagData) String() string {
	return fmt.Sprintf("Digests: %v", t.Digests)
}

// accepts returns true if request has Accept header for the given media type
func accepts(ctx context.Context, mediaType string) bool {
	req, _ := rctx.GetRequest(ctx)
	if req != nil {
		if hdr, ok := req.Header["Accept"]; ok {
			for _, s := range hdr {
				if s == mediaType {
					return true
				}
			}
		}
	}
	return false
}

// ManifestList determines what type of media type is accepted, and returns the digest and media type
func (t *tagData) ManifestLink(ctx context.Context) (string, string) {
	if accepts(ctx, ManifestListMediaType) {
		for dg, t := range t.Digests {
			if t == ManifestListMediaType {
				return dg, t
			}
		}
	}
	if accepts(ctx, ManifestV2MediaType) {
		for dg, t := range t.Digests {
			if t == ManifestV2MediaType {
				return dg, t
			}
		}
	}
	for dg, t := range t.Digests {
		if t == ManifestV1MediaType {
			return dg, t
		}
	}
	// If we're here, there is no v1 schema.
	for dg, t := range t.Digests {
		if t == ManifestV2MediaType {
			return dg, t
		}
	}
	for dg, t := range t.Digests {
		if t == ManifestListMediaType {
			return dg, t
		}
	}

	return "", ""
}

func readMetadataFile(fname string) (repoMetadata, error) {
	var rmd repoMetadata
	rmd.Filename = fname
	data, err := ioutil.ReadFile(fname)
	if err != nil {
		return rmd, err
	}
	err = json.Unmarshal(data, &rmd)
	return rmd, nil
}

func processMetadata(rmd *repoMetadata) *repository {
	parts := strings.Split(rmd.RepoId, "/")
	if parts[0] == "library" {
		parts = parts[1:]
		rmd.RepoId = strings.Join(parts, "/")
	}
	if rmd.Version == 2 {
		return processV2Metadata(rmd)
	} else if rmd.Version == 3 || rmd.Version == 4 {
		return processV4Metadata(rmd)
	}
	return nil
}

type tagList struct {
	Name string   `json:"name"`
	Tags []string `json:"tags"`
}

func processV2Metadata(rmd *repoMetadata) *repository {
	var repo repository
	repo.Name = rmd.RepoId
	repo.RepoMd = rmd
	repo.Tags = make(map[string]*tagData)
	// Tags are not in md, fetch them from the md URL
	tags, _, err := httpGetContent(joinUrl(rmd.Url, "tags", "list"))
	if err == nil {
		var td tagList
		if json.Unmarshal(tags, &td) == nil {
			for _, tag := range td.Tags {
				s := tag
				go func() {
					manifest, _, err := httpGetContent(joinUrl(rmd.Url, "manifests", s))
					if err == nil {
						repo.Tags[s] = &tagData{Digests: make(map[string]string)}
						processManifestData(manifest, s, repo.Tags[s])
					} else {
						fmt.Printf("Cannot retrieve %s\n", joinUrl(rmd.Url, "manifests", s))
					}
				}()
			}
			return &repo
		}
	}
	return nil
}

func processV4Metadata(rmd *repoMetadata) *repository {
	var repo repository
	repo.RepoMd = rmd
	repo.Name = rmd.RepoId
	repo.Tags = make(map[string]*tagData)
	for _, tag := range rmd.Schema2Data {
		if _, err := digest.Parse(tag); err != nil {
			// A tag
			manifest, _, err := httpGetContent(joinUrl(rmd.Url, "manifests", "2", tag))
			if err == nil {
				repo.Tags[tag] = &tagData{Digests: make(map[string]string)}
				processManifestData(manifest, tag, repo.Tags[tag])
			} else {
				fmt.Printf("Cannot retrieve %s: %s\n", joinUrl(rmd.Url, "manifests", "2", tag), err)
			}
		}
	}
	for _, tag := range rmd.ManifestListData {
		if _, err := digest.Parse(tag); err != nil {
			// A tag
			manifest, _, err := httpGetContent(joinUrl(rmd.Url, "manifests", "list", tag))
			if err == nil {
				repo.Tags[tag] = &tagData{Digests: make(map[string]string)}
				processManifestData(manifest, tag, repo.Tags[tag])
			} else {
				fmt.Printf("Cannot retrieve %s: %s\n", joinUrl(rmd.Url, "manifests", "list", tag), err)
			}
		}
	}

	for tag, data := range rmd.Amd64Tags {
		//digestStr := fmt.Sprint(data[0])
		versionStr := fmt.Sprint(data[1])
		manifest, _, err := httpGetContent(joinUrl(rmd.Url, "manifests", versionStr, tag))
		if err == nil {
			repo.Tags[tag] = &tagData{Digests: make(map[string]string)}
			processManifestData(manifest, tag, repo.Tags[tag])
		} else {
			fmt.Printf("Cannot retrieve %s: %s\n", joinUrl(rmd.Url, "manifests", "list", tag), err)
		}
	}
	return &repo
}

type manifestV1 struct {
	Name   string    `json:"name"`
	Tag    string    `json:"tag"`
	Arch   string    `json:"architecture"`
	Layers []blobSum `json:"fsLayers"`
}

type blobSum struct {
	Digest string `json:"blobSum"`
}

type manifestV2 struct {
	MediaType string      `json:"mediaType"`
	Config    digestRef   `json:"config"`
	Layers    []digestRef `json:"layers"`
}

type digestRef struct {
	MediaType string `json:"mediaType"`
	Size      int    `json:"size"`
	Digest    string `json:"digest"`
}

type manifestList struct {
	MediaType string `json:"mediaType"`
	Manifests []manifestListItem
}

type manifestListItem struct {
	MediaType string `json:"mediaType"`
	Size      int    `json:"size"`
	Digest    string `json:"digest"`
	Platform  struct {
		Arch string `json:"architecture"`
		Os   string `json:"os"`
	} `json:"platform"`
}

func processManifestData(manifest []byte, tag string, tdata *tagData) {
	var manifestData map[string]interface{}
	if err := json.Unmarshal(manifest, &manifestData); err == nil {
		manifestDigest := digest.FromBytes(manifest)
		schemaVersion, ok := getInt(manifestData, "schemaVersion")
		if !ok {
			fmt.Printf("Cannot get schema version from manifest: %v", manifestData)
		}
		switch schemaVersion {
		case 1:
			var v1Data manifestV1
			if err = json.Unmarshal(manifest, &v1Data); err == nil {
				tdata.Digests[manifestDigest.String()] = ManifestV1MediaType
				for _, layer := range v1Data.Layers {
					tdata.Digests[layer.Digest] = LayerMediaType
				}
			}
		case 2:
			if mediaType, ok := getStr(manifestData, "mediaType"); ok {
				if mediaType == ManifestListMediaType {
					// Manifest list
					var list manifestList
					if err = json.Unmarshal(manifest, &list); err == nil {
						tdata.Digests[manifestDigest.String()] = ManifestListMediaType
					}
				} else {
					// Manifest
					var v2Data manifestV2
					if err = json.Unmarshal(manifest, &v2Data); err == nil {
						tdata.Digests[manifestDigest.String()] = ManifestV2MediaType
						for _, layer := range v2Data.Layers {
							tdata.Digests[layer.Digest] = LayerMediaType
						}
					}
				}
			} else {
				err = fmt.Errorf("Cannot get mediaType for %s", string(manifest))
			}
		}
		if err != nil {
			fmt.Printf("Error processing manifest: %s", err)
		}
	} else {
		fmt.Printf("Cannot parse manifest: %s\n", err.Error())
	}
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

// getStr gets a string from the map
func getStr(m map[string]interface{}, key string) (string, bool) {
	if v, ok := m[key]; ok {
		return fmt.Sprint(v), true
	}
	return "", false
}

// Contains the file information, and the repoid described in this file
type fileMapping struct {
	info   os.FileInfo
	repoId string
}

type pulpMetadata struct {
	sync.RWMutex
	files map[string]fileMapping
	repos map[string]*repository
}

func (md *pulpMetadata) FindRepoByName(name string) *repository {
	r, ok := md.repos[name]
	if ok {
		return r
	}
	return nil
}

// Returns the repository and the tag containing the digest. Returns true if digest is a layer blob
func (md *pulpMetadata) FindByDigest(dg digest.Digest) (*repository, string, bool) {
	dgs := dg.String()
	for _, r := range md.repos {
		for tag, data := range r.Tags {
			if m, ok := data.Digests[dgs]; ok {
				return r, tag, m == LayerMediaType
			}
		}
	}
	return nil, "", false
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
		md.RLock()
		info, ok := md.files[f.Name()]
		md.RUnlock()
		fileModified := false
		if ok {
			if !info.info.ModTime().Equal(f.ModTime()) {
				fileModified = true
			}
		} else {
			fileModified = true
		}
		if fileModified {
			changed = true
			rmd, err := readMetadataFile(path.Join(dir, f.Name()))
			if err == nil {
				fmapping := fileMapping{info: f, repoId: rmd.RepoId}
				wg.Add(1)
				go func() {
					defer wg.Done()
					repo := processMetadata(&rmd)
					if repo != nil {
						md.Lock()
						defer md.Unlock()
						md.files[f.Name()] = fmapping
						md.repos[rmd.RepoId] = repo
					}
				}()
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
