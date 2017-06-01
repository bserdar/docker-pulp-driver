package pulp

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
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
		pollingInterval uint64 = 0
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
			pollingInterval = 0
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

// GetContent retrieves the content stored at "path" as a []byte.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	context.GetLogger(ctx).Debugf("getContent: path:%s", path)

	rr, err := getRequestType(ctx, path)
	if err != nil {
		return nil, err
	}
	if rr != nil {
		switch t := rr.(type) {
		case manifestIndexRequest:
			context.GetLogger(ctx).Debugf("getContent: path:%s manifestIndex", path)
			return t.getManifestIndex()
		case manifestRequest:
			context.GetLogger(ctx).Debugf("getContent: path:%s manifest", path)
			return t.getManifest()
		case layerLinkRequest:
			context.GetLogger(ctx).Debugf("getContent: path:%s layer link", path)
			return t.getLink()
		}
	}
	return nil, storagedriver.PathNotFoundError{Path: path}
}

// PutContent stores the []byte content at a location designated by "path".
func (d *driver) PutContent(ctx context.Context, subPath string, contents []byte) error {
	return storagedriver.ErrUnsupportedMethod{}
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	return nil, storagedriver.ErrUnsupportedMethod{}
}

func (d *driver) Writer(ctx context.Context, subPath string, append bool) (storagedriver.FileWriter, error) {
	return nil, storagedriver.ErrUnsupportedMethod{}
}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, subPath string) (storagedriver.FileInfo, error) {
	context.GetLogger(ctx).Debugf("stat: path:%s", subPath)

	rr, err := getRequestType(ctx, subPath)
	if err != nil {
		return nil, err
	}
	ret := storagedriver.FileInfoInternal{}
	ret.FileInfoFields.Path = subPath
	ret.FileInfoFields.IsDir = false
	switch t := rr.(type) {
	case layerLinkRequest:
		ret.FileInfoFields.Size = 71
		ret.FileInfoFields.ModTime = time.Time{}
		context.GetLogger(ctx).Debugf("stat: path:%s layerLink", subPath)
		return ret, nil
	case manifestIndexRequest:
		ret.FileInfoFields.Size = 71
		ret.FileInfoFields.ModTime = time.Time{}
		context.GetLogger(ctx).Debugf("stat: path:%s manifestIndex", subPath)
		return ret, nil
	case manifestRequest:
		fd, ok := pulpMetadata.repos[t.GetName()]
		if ok {
			context.GetLogger(ctx).Debugf("stat: path:%s name: %s ref: %s", subPath, fd.repoMd.RepoId,
				t.hash)
			tagInfo, ok := fd.getTagInfoByHash(t.hash)
			if ok {
				ret.FileInfoFields.Size = tagInfo.size
				ret.FileInfoFields.ModTime = tagInfo.modTime
				context.GetLogger(ctx).Debugf("stat: path:%s manifest", subPath)
				return ret, nil
			}
		}
	case layerRequest:
		fd, ok := pulpMetadata.repos[t.GetName()]
		if ok {
			context.GetLogger(ctx).Debugf("stat: path:%s layer", subPath)
			return getBlobHeader(fd.Url, t.digest)
		}
	}
	return nil, storagedriver.PathNotFoundError{Path: subPath}
}

// List returns a list of the objects that are direct descendants of the given
// path.
func (d *driver) List(ctx context.Context, subPath string) ([]string, error) {
	return nil, storagedriver.ErrUnsupportedMethod{}
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
	context.GetLogger(ctx).Debugf("URLFor :%s", path)
	rr, err := getRequestType(ctx, path)
	context.GetLogger(ctx).Debugf("URLFor :%s rq: %v", path, rr)

	if err != nil {
		return "", err
	}

	if rr == nil {
		return "", storagedriver.PathNotFoundError{Path: path}
	}

	fd, ok := pulpMetadata.repos[rr.GetName()]
	if !ok {
		return "", storagedriver.PathNotFoundError{Path: path}
	}
	switch t := rr.(type) {
	case layerRequest:
		context.GetLogger(ctx).Debugf("URLFor :%s layerRequest", path)
		return joinUrl(fd.Url, "blobs", t.digest), nil
	case manifestRequest:
		ti, ok := fd.getTagInfoByHash(t.hash)
		if ok {
			context.GetLogger(ctx).Debugf("URLFor :%s manifestRequest", path)
			return joinUrl(fd.Url, "manifests", ti.tag), nil
		}
	}
	return "", storagedriver.PathNotFoundError{Path: path}
}

func getImageManifest(url, tag string) ([]byte, *http.Response, error) {
	return httpGetContent(joinUrl(url, "manifests", tag))
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

func getBlobHeader(url, digest string) (storagedriver.FileInfo, error) {
	resp, err := httpReq(joinUrl(url, "blobs", digest), "HEAD", 0)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	fi := storagedriver.FileInfoInternal{}
	fi.FileInfoFields.Path = joinUrl(url, "blobs", digest)
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

func getImageTags(url string) ([]string, error) {
	return nil, nil
}

// Base interface for all registry requests
type registryRequest interface {
	GetName() string
}

// manifestRequest is the request to retrieve the manifest for the given name and reference
type manifestRequest struct {
	name string
	hash string
}

func (r manifestRequest) GetName() string { return r.name }

func (r manifestRequest) getManifest() ([]byte, error) {
	fd := pulpMetadata.repos[r.name]
	if fd != nil {
		ti, ok := fd.getTagInfoByHash(r.hash)
		if ok {
			data, _, err := getImageManifest(fd.Url, ti.tag)
			if err != nil {
				return nil, err
			}
			return data, nil
		}
	}
	return nil, storagedriver.PathNotFoundError{Path: r.name}
}

type layerLinkRequest struct {
	name string
	hash string
}

func (r layerLinkRequest) GetName() string { return r.name }

func (r layerLinkRequest) getLink() ([]byte, error) {
	return []byte(r.hash), nil
}

// manifestIndexRequest is the request to retrieve a pointer to the manifest
type manifestIndexRequest struct {
	name      string
	reference string
}

func (r manifestIndexRequest) GetName() string { return r.name }

func (r manifestIndexRequest) getManifestIndex() ([]byte, error) {
	fd := pulpMetadata.repos[r.name]
	if fd != nil {
		hash, ok := fd.getHashByTag(r.reference)
		if !ok {
			// Read manifest, compute hash
			data, resp, err := getImageManifest(fd.Url, r.reference)
			if err != nil {
				return nil, err
			}
			hash = fd.addTag(r.reference, data, getModTime(resp))
		}
		return []byte(hash), nil
	}
	return nil, storagedriver.PathNotFoundError{Path: r.name}
}

// layerRequest is the request to request a layer download/upload
type layerRequest struct {
	name   string
	upload bool
	digest string
}

func (r layerRequest) GetName() string { return r.name }

func makeName(segments []string) string {
	begin := 0
	if segments[0] == "library" {
		begin = 1
	}
	return joinUrl(segments[begin:]...)
}

func getRequestType(ctx context.Context, path string) (registryRequest, error) {
	var name, reference string

	x := ctx.Value("vars.name")
	if x != nil {
		name = x.(string)
		name = strings.TrimPrefix(name, "library/")
	}
	x = ctx.Value("vars.reference")
	if x != nil {
		reference = x.(string)
	}

	fd, ok := pulpMetadata.repos[name]
	if !ok {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}

	if strings.HasPrefix(path, registryRoot) {
		segments := strings.Split(path[len(registryRoot):], string(os.PathSeparator))
		context.GetLogger(ctx).Debugf("getRequestType : %s:%s %v", name, reference, segments)
		if segments[0] == "blobs" {
			// Are we loading manifest, or a layer?
			digest := segments[1] + ":" + segments[3]
			fd := findFileDataByManifestHash(digest)
			if fd != nil {
				// Loading manifest
				r := manifestRequest{name: fd.RepoId, hash: digest}
				context.GetLogger(ctx).Debugf("getRequestType manifestRequest :%v", r)
				return r, nil
			} else {
				// Loading layer
				r := layerRequest{name: name, digest: digest}
				context.GetLogger(ctx).Debugf("getRequestType layerRequest :%v", r)
				return r, nil
			}
		} else if segments[0] == "repositories" {
			segments = segments[1:]
			// segments[0] to (_manifests,_uploads,_layers) is the name
			for i, x := range segments {
				if x == "_manifests" || x == "_uploads" || x == "_layers" {
					segments = segments[i:]
					break
				}
			}
			n := len(segments)
			if segments[0] == "_manifests" {
				if segments[1] == "tags" {
					if n > 1 {
						tag := segments[2]
						if n > 2 {
							if segments[3] == "current" && segments[4] == "link" {
								return manifestIndexRequest{name: name, reference: tag}, nil
							} else if segments[3] == "index" {
								return manifestIndexRequest{name: name, reference: tag}, nil
							}
						}
					}
				} else if segments[1] == "revisions" {
					hash := segments[2] + ":" + segments[3]
					tag, ok := fd.getTagByHash(hash)
					if ok {
						return manifestIndexRequest{name: name, reference: tag}, nil
					}
				}
			} else if segments[0] == "_layers" {
				return layerLinkRequest{name: name, hash: segments[1] + ":" + segments[2]}, nil
			}
		}
	}
	return nil, nil
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
