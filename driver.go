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
	"time"

	rctx "github.com/docker/distribution/context"
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

	x := pulpMd.fs.Find(path)
	if x != nil && !x.isDir() {
		file := x.(*mfile)
		if file.isRedirect {
			if file.isLayer {
				panic("Cannot read layer")
			} else {
				data, _, err := httpGetContent(file.redirectUrl)
				if err != nil {
					return nil, err
				}
				return NewReaderCloser(string(data)), nil
			}
		} else {
			return NewReaderCloser(string(file.contents)), nil
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

	x := pulpMd.fs.Find(subPath)
	if x != nil {
		if !x.isDir() {
			file := x.(*mfile)
			if file.isRedirect {
				return getBlobHeader(ctx, subPath, file.redirectUrl)
			}
		}
		ret := storagedriver.FileInfoInternal{}
		ret.FileInfoFields.Path = subPath
		ret.FileInfoFields.IsDir = x.isDir()
		if !x.isDir() {
			ret.FileInfoFields.Size = x.(*mfile).size
		}
		ret.FileInfoFields.ModTime = x.getModTime()
		return ret, nil
	}
	return nil, storagedriver.PathNotFoundError{Path: subPath}
}

// List returns a list of the objects that are direct descendants of the given
// path.
func (d *driver) List(ctx context.Context, subPath string) ([]string, error) {
	fullPath := subPath
	rctx.GetLogger(ctx).Debugf("list: path: %s, fullPath: %s\n", subPath, fullPath)
	x := pulpMd.fs.Find(fullPath)

	if x != nil && x.isDir() {
		ret := make([]string, 0)
		for _, node := range x.(*mdirectory).nodes {
			ret = append(ret, path.Join(subPath, node.getName()))
		}
		return ret, nil
	}
	return nil, storagedriver.PathNotFoundError{Path: subPath}
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
	x := pulpMd.fs.Find(path)
	if x != nil {
		if !x.isDir() {
			file := x.(*mfile)
			if file.isRedirect {
				return file.redirectUrl, nil
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
