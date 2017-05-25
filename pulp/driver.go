package pulp

import (
	"fmt"
	"io"
	"reflect"
	"strconv"

	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
	fsd "github.com/docker/distribution/registry/storage/driver/filesystem"
)

// Copied from filesystem driver

const (
	driverName                 = "pulp"
	defaultRootDirectory       = "/var/lib/registry"
	defaultMaxThreads          = uint64(100)
	defaultPollingIntervalSecs = uint64(60)

	// minThreads is the minimum value for the maxthreads configuration
	// parameter. If the driver's parameters are less than this we set
	// the parameters to minThreads
	minThreads = uint64(25)
)

// DriverParameters represents all configuration options available for the
// filesystem driver
type DriverParameters struct {
	RootDirectory       string
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
	return FromParameters(parameters)
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

// FromParameters constructs a new Driver with a given parameters map
// Optional Parameters:
// - rootdirectory
// - maxthreads
func FromParameters(parameters map[string]interface{}) (*Driver, error) {
	params, err := fromParametersImpl(parameters)
	if err != nil || params == nil {
		return nil, err
	}
	return New(*params), nil
}

func fromParametersImpl(parameters map[string]interface{}) (*DriverParameters, error) {
	var (
		maxThreads             = defaultMaxThreads
		pollingInterval uint64 = 0
		rootDirectory          = defaultRootDirectory
		pollingDir             = ""
	)

	if parameters != nil {
		if rootDir, ok := parameters["rootdirectory"]; ok {
			rootDirectory = fmt.Sprint(rootDir)
		}
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

	params := &DriverParameters{
		RootDirectory:       rootDirectory,
		MaxThreads:          maxThreads,
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
		RootDirectory: params.RootDirectory,
		MaxThreads:    params.MaxThreads}

	fsDriver := fsd.New(fsParams)
	driver := &driver{rootDirectory: params.RootDirectory,
		pollingDir: params.PollingDir,
		fsDriver:   fsDriver}
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
	//	return "", storagedriver.ErrUnsupportedMethod{}
	return "http://somewhere", nil
}
