package pulp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	rctx "github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
	digest "github.com/opencontainers/go-digest"
)

// Copied from filesystem driver

const (
	driverName                 = "pulp"
	defaultMaxThreads          = uint64(100)
	defaultPollingIntervalSecs = uint64(60)
	registryRoot               = "/docker/registry/v2/"

	// minThreads is the minimum value for the maxthreads configuration
	// parameter. If the driver's parameters are less than this we set
	// the parameters to minThreads
	minThreads = uint64(25)
)

var (
	metadata    = pulpMetadata{files: map[string]fileMapping{}, repos: map[string]*repository{}}
	updateMx    sync.Mutex
	defaultTime = time.Now()
)

// DriverParameters represents all configuration options available for the
// filesystem driver
type DriverParameters struct {
	MaxThreads          uint64
	PollingIntervalSecs uint64
	PollingDir          string
}

func init() {
	factory.Register(driverName, &pulpDriverFactory{})
}

// pulpDriverFactory implements the factory.StorageDriverFactory interface
type pulpDriverFactory struct{}

func (factory *pulpDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	params, err := parseParameters(parameters)
	if err != nil {
		return nil, err
	}
	updateMd(params.PollingDir)
	startDirWatch(params.PollingDir, params.PollingIntervalSecs)
	return New(*params), nil
}

func updateMd(dir string) {
	updateMx.Lock()
	defer updateMx.Unlock()
	fmt.Printf("UpdateMd %s\n", dir)
	changed, _ := metadata.scanDir(dir)
	if changed {
		fmt.Printf("%s\n", metadata.repos)
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

type driver struct {
	rootDirectory string
	pollingDir    string
}

type baseEmbed struct {
	base.Base
}

// Driver is a storagedriver.StorageDriver implementation backed by a
// local filesystem containing pulp-generated metadata files.
type Driver struct {
	baseEmbed
}

func parseParameters(parameters map[string]interface{}) (*DriverParameters, error) {
	var (
		maxThreads             = defaultMaxThreads
		pollingInterval uint64 = 60
		pollingDir             = ""
	)

	if parameters != nil {
		if watchDir, ok := parameters["pollingdir"]; ok {
			pollingDir = fmt.Sprint(watchDir)
		}

		// Get maximum number of threads for blocking filesystem operations,
		// if specified
		maxThreads, _ = getUintValue("maxthreads", parameters["maxthreads"])
		if maxThreads < minThreads {
			maxThreads = minThreads
		}
		pollingInterval, _ = getUintValue("pollingInterval", parameters["pollingInterval"])
		if pollingInterval <= 0 {
			pollingInterval = 60
		}
	}

	params := &DriverParameters{MaxThreads: maxThreads,
		PollingIntervalSecs: pollingInterval,
		PollingDir:          pollingDir,
	}
	return params, nil
}

func getUintValue(valueName string, value interface{}) (uint64, error) {
	var ret uint64
	var err error
	switch v := value.(type) {
	case string:
		if ret, err = strconv.ParseUint(v, 0, 64); err != nil {
			return 0, fmt.Errorf("%s parameter must be an integer, %v invalid", valueName, value)
		}
	case uint64:
		ret = v
	case int, int32, int64:
		val := reflect.ValueOf(v).Convert(reflect.TypeOf(value)).Int()
		if val > 0 {
			ret = uint64(val)
		}
	case uint, uint32:
		ret = reflect.ValueOf(v).Convert(reflect.TypeOf(value)).Uint()
	case nil:
		// do nothing
	default:
		return 0, fmt.Errorf("invalid value for %s: %#v", valueName, value)
	}
	return ret, nil
}

// New constructs a new Driver with a given rootDirectory
func New(params DriverParameters) *Driver {
	driver := &driver{pollingDir: params.PollingDir}
	updateMd(params.PollingDir)
	startDirWatch(params.PollingDir, params.PollingIntervalSecs)

	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: base.NewRegulator(driver, params.MaxThreads),
			},
		},
	}
}

// Implement the storagedriver.StorageDriver interface

func (d *driver) Name() string {
	return driverName
}

type StrReaderCloser struct {
	reader *strings.Reader
}

func (s StrReaderCloser) Close() error               { return nil }
func (s StrReaderCloser) Read(b []byte) (int, error) { return s.reader.Read(b) }

func NewReaderCloser(s string) StrReaderCloser {
	var r StrReaderCloser
	r.reader = strings.NewReader(s)
	return r
}

// GetContent retrieves the content stored at "path" as a []byte.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	rctx.GetLogger(ctx).Debugf("getContent: path:%s", path)
	rc, err := d.Reader(ctx, path, 0)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	p, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, err
	}

	return p, nil
}

// PutContent stores the []byte content at a location designated by "path".
func (d *driver) PutContent(ctx context.Context, subPath string, contents []byte) error {
	return storagedriver.ErrUnsupportedMethod{}
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	rctx.GetLogger(ctx).Debugf("reader: path:%s", path)
	// Get the significant parts of the path
	components := getPathComponents(path)
	rctx.GetLogger(ctx).Debugf("components: %v", components)
	if len(components) > 0 {
		if components[0] == "blobs" {
			if len(components) > 4 {
				metadata.RLock()
				defer metadata.RUnlock()

				dg := digest.NewDigestFromHex(components[1], components[3])
				repo, tag, isLayer := metadata.FindByDigest(dg)
				if repo != nil {
					if isLayer {
						return nil, fmt.Errorf("Cannot read layer")
					}
					_, ok := repo.Tags[tag]
					if ok {
						// Read the manifest
						data, _, err := httpGetContent(joinUrl(repo.RepoMd.Url, "blobs", dg.String()))
						if err != nil {
							return nil, err
						}
						return NewReaderCloser(string(data)), nil
					}
				}
			}
		} else if components[0] == "repositories" {
			// components 1 and possibly 2 have the name, then _manifests or _layers
			var name string
			for i, c := range components {
				if c == "_manifests" || c == "_layers" {
					name = strings.Join(components[1:i], "/")
					components = components[i:]
					break
				}
			}
			rctx.GetLogger(ctx).Debugf("name: %s", name)
			if len(name) > 0 && len(components) > 1 {
				repo := metadata.FindRepoByName(name)
				rctx.GetLogger(ctx).Debugf("repo: %v", repo)
				if repo != nil {
					if components[0] == "_manifests" {
						if len(components) > 1 {
							if components[1] == "revisions" {
								if len(components) > 4 {
									return NewReaderCloser(string(digest.NewDigestFromHex(components[2], components[3]))), nil
								}
							} else if components[1] == "tags" {
								if len(components) > 2 {
									tag := components[2]
									rctx.GetLogger(ctx).Debugf("Tag: %s", tag)
									tagdata, ok := repo.Tags[tag]
									if ok {
										components = components[3:]
										rctx.GetLogger(ctx).Debugf("Remaining components: %v", components)
										if len(components) > 0 {
											if components[0] == "current" {
												s, _ := tagdata.ManifestLink(ctx)
												rctx.GetLogger(ctx).Debugf("Return: %s", s)
												return NewReaderCloser(s), nil
											} else if components[0] == "index" {
												s, _ := tagdata.ManifestLink(ctx)
												return NewReaderCloser(s), nil
											}
										}
									}
								}
							}
						}
					} else if components[0] == "_layers" {
						if len(components) > 3 {
							// Return the digest as file contents
							return NewReaderCloser(string(digest.NewDigestFromHex(components[1], components[2]))), nil
						}
					}
				}
			}
		}
	}
	return nil, storagedriver.PathNotFoundError{Path: path}
}

func (d *driver) Writer(ctx context.Context, subPath string, append bool) (storagedriver.FileWriter, error) {
	return nil, storagedriver.ErrUnsupportedMethod{}
}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, subPath string) (storagedriver.FileInfo, error) {
	rctx.GetLogger(ctx).Debugf("stat: path:%s", subPath)
	ret := storagedriver.FileInfoInternal{}
	ret.FileInfoFields.Path = subPath
	ret.FileInfoFields.ModTime = defaultTime
	ret.FileInfoFields.IsDir = true
	// We set size and dir flags
	components := getPathComponents(subPath)
	if len(components) > 0 {
		if components[0] == "blobs" {
			if len(components) > 4 {
				rctx.GetLogger(ctx).Debugf("stat: blob components:%v", components)
				metadata.RLock()
				defer metadata.RUnlock()
				dg := digest.NewDigestFromHex(components[1], components[3])
				repo, tag, isLayer := metadata.FindByDigest(dg)
				if repo != nil {
					if isLayer {
						// Get layer info
						return getBlobHeader(ctx, subPath, joinUrl(repo.RepoMd.Url, "blobs", dg.String()))
					}
					_, ok := repo.Tags[tag]
					if ok {
						// Get the manifest info
						return getBlobHeader(ctx, subPath, joinUrl(repo.RepoMd.Url, "blobs", dg.String()))
					}
				}
			}
		} else if components[0] == "repositories" {
			// components 1 and possibly 2 have the name, then _manifests or _layers
			var name string
			for i, c := range components {
				if c == "_manifests" || c == "_layers" {
					name = strings.Join(components[1:i], "/")
					components = components[i:]
					break
				}
			}
			if len(name) > 0 && len(components) > 1 {
				repo := metadata.FindRepoByName(name)
				if repo != nil {
					if components[0] == "_manifests" {
						if len(components) > 1 {
							if components[1] == "revisions" {
								if len(components) > 4 {
									ret.FileInfoFields.IsDir = false
									ret.FileInfoFields.Size = 71
								}
							} else if components[1] == "tags" {
								if len(components) > 2 {
									tag := components[2]
									_, ok := repo.Tags[tag]
									if ok {
										components = components[3:]
										if len(components) > 0 {
											if components[0] == "current" {
												ret.FileInfoFields.IsDir = false
												ret.FileInfoFields.Size = 71
											} else if components[0] == "index" {
												ret.FileInfoFields.IsDir = false
												ret.FileInfoFields.Size = 71
											}
										}
									}
								}
							}
						}
					} else if components[0] == "_layers" {
						if len(components) > 3 {
							ret.FileInfoFields.IsDir = false
							ret.FileInfoFields.Size = 71
						}
					}
				}
			}
		}
	}
	return ret, nil
}

// List returns a list of the objects that are direct descendants of the given
// path.
func (d *driver) List(ctx context.Context, subPath string) ([]string, error) {
	fullPath := subPath
	rctx.GetLogger(ctx).Debugf("list: path: %s, fullPath: %s\n", subPath, fullPath)
	ret := make([]string, 0)
	// Implement list only for tags
	components := getPathComponents(subPath)
	if len(components) > 4 {
		if components[0] == "repositories" {
			var name string
			for i, c := range components {
				if c == "_manifests" || c == "_layers" {
					name = strings.Join(components[1:i], "/")
					components = components[i:]
					break
				}
			}
			if len(name) > 0 && len(components) == 2 {
				repo := metadata.FindRepoByName(name)
				if repo != nil {
					if components[0] == "_manifests" && components[1] == "tags" {
						for t, _ := range repo.Tags {
							ret = append(ret, path.Join(subPath, t))
						}
					}
				}
			}
		}
	}
	return ret, nil
}

// Move moves an object stored at sourcePath to destPath, removing the original
// object.
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	return storagedriver.ErrUnsupportedMethod{}
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, subPath string) error {
	return storagedriver.ErrUnsupportedMethod{}
}

// URLFor returns a URL which may be used to retrieve the content stored at the given path.
// May return an UnsupportedMethodErr in certain StorageDriver implementations.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	rctx.GetLogger(ctx).Debugf("URLFor :%s", path)
	components := getPathComponents(path)
	if len(components) > 0 {
		if components[0] == "blobs" {
			if len(components) > 4 {
				metadata.RLock()
				defer metadata.RUnlock()

				dg := digest.NewDigestFromHex(components[1], components[3])
				repo, tag, isLayer := metadata.FindByDigest(dg)
				if repo != nil {
					if isLayer {
						return joinUrl(repo.RepoMd.Url, "blobs", dg.String()), nil
					}
					_, ok := repo.Tags[tag]
					if ok {
						_, mediaType := repo.Tags[tag].ManifestLink(ctx)
						if mediaType == ManifestListMediaType {
							return joinUrl(repo.RepoMd.Url, "manifests", "list", tag), nil
						} else if mediaType == ManifestV2MediaType {
							return joinUrl(repo.RepoMd.Url, "manifests", "2", tag), nil
						}
						return joinUrl(repo.RepoMd.Url, "manifest", "1", tag), nil
					}
				}
			}
		}
	}
	return "", storagedriver.PathNotFoundError{Path: path}
}

func getModTime(resp *http.Response) time.Time {
	lastModified := resp.Header.Get("last-modified")
	if len(lastModified) > 0 {
		t, err := time.Parse("Mon, 2 Jan 2006 15:04:05 MST", lastModified)
		if err != nil {
			return t
		}
	}
	return time.Time{}
}

func getBlobHeader(ctx context.Context, path, url string) (storagedriver.FileInfo, error) {
	resp, err := httpReq(url, "HEAD", 0)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	fi := storagedriver.FileInfoInternal{}
	fi.FileInfoFields.Path = path
	fi.FileInfoFields.ModTime = getModTime(resp)
	length := resp.Header.Get("content-length")
	if len(length) > 0 {
		i, err := strconv.Atoi(length)
		if err == nil {
			fi.FileInfoFields.Size = int64(i)
		}
	}
	return fi, nil
}

func joinUrl(parts ...string) string {
	buf := bytes.Buffer{}
	lastIsSlash := true // Do not put a / before the url
	for _, part := range parts {
		if lastIsSlash {
			if part[0] == '/' {
				buf.WriteString(part[1:])
			} else {
				buf.WriteString(part)
			}
		} else {
			if part[0] == '/' {
				buf.WriteString(part)
			} else {
				buf.WriteRune('/')
				buf.WriteString(part)
			}
		}
		lastIsSlash = part[len(part)-1] == '/'
	}
	return buf.String()
}

func httpReq(url, method string, offset uint64) (*http.Response, error) {
	request, err := http.NewRequest(method, url, nil)
	if offset > 0 {
		request.Header.Add("Range", fmt.Sprintf("bytes:%d-", offset))
	}
	client := &http.Client{}
	resp, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// httpRead reads a file at the url, optionally with an offset
func httpRead(url string, offset uint64) (*http.Response, error) {
	return httpReq(url, "GET", offset)
}

func httpGetContent(url string) ([]byte, *http.Response, error) {
	resp, err := httpRead(url, 0)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	p, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}
	return p, resp, nil
}

func getPathComponents(p string) []string {
	parts := strings.Split(p, "/")
	if len(parts) > 0 && len(parts[0]) == 0 {
		parts = parts[1:]
	}
	if len(parts) > 3 && parts[0] == "docker" && parts[1] == "registry" && parts[2] == "v2" {
		parts = parts[3:]
	}
	return parts
}
