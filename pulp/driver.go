package pulp

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"

	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
	fsd "github.com/docker/distribution/registry/storage/driver/filesystem"
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
	fsDriver      *fsd.Driver
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
	fsParams := fsd.DriverParameters{
		MaxThreads: params.MaxThreads}

	fsDriver := fsd.New(fsParams)
	driver := &driver{pollingDir: params.PollingDir,
		fsDriver: fsDriver}
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
	redirect, err := getRedirectPath(ctx, path)
	fmt.Printf("path: %s redirectPath: %s\n", path, redirect)
	if err != nil {
		return nil, err
	}
	return d.fsDriver.GetContent(ctx, path)
}

// PutContent stores the []byte content at a location designated by "path".
func (d *driver) PutContent(ctx context.Context, subPath string, contents []byte) error {
	return d.fsDriver.PutContent(ctx, subPath, contents)
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	return d.fsDriver.Reader(ctx, path, offset)
}

func (d *driver) Writer(ctx context.Context, subPath string, append bool) (storagedriver.FileWriter, error) {
	return d.fsDriver.Writer(ctx, subPath, append)
}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, subPath string) (storagedriver.FileInfo, error) {
	return d.fsDriver.Stat(ctx, subPath)
}

// List returns a list of the objects that are direct descendants of the given
// path.
func (d *driver) List(ctx context.Context, subPath string) ([]string, error) {
	return d.fsDriver.List(ctx, subPath)
}

// Move moves an object stored at sourcePath to destPath, removing the original
// object.
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	return d.fsDriver.Move(ctx, sourcePath, destPath)
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, subPath string) error {
	return d.fsDriver.Delete(ctx, subPath)
}

// URLFor returns a URL which may be used to retrieve the content stored at the given path.
// May return an UnsupportedMethodErr in certain StorageDriver implementations.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	return getRedirectPath(ctx, path)
}

func getRedirectPath(ctx context.Context, path string) (string, error) {
	context.GetLogger(ctx).Debugf("getRedirectPath(%s)", path)
	if !strings.HasPrefix(path, registryRoot) {
		return "", storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
	}
	segments := strings.Split(path[len(registryRoot):], string(os.PathSeparator))
	context.GetLogger(ctx).Debug("getRedirectPath segments:%v", segments)
	if segments[0] == "blob" {
	} else if segments[0] == "repositories" {
		// segments[1] to (_manifests,_uploads,_layers) is the name
		name := ""
		for i, x := range segments {
			if x == "_manifests" || x == "_uploads" || x == "_layers" {
				begin := 0
				if segments[0] == "library" {
					begin = 1
				}
				name = filepath.Join(segments[begin:i]...)
				break
			}
		}
		context.GetLogger(ctx).Debugf("getRedirectPath: name:%s", name)
		if name == "" {
			return "", storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
		}
		fd, ok := pulpMetadata.repos[name]
		if ok {
			return fd.Url, nil
		} else {
			context.GetLogger(ctx).Warnf("No pulp repo for %s", name)
		}
	}
	return "", storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
}
