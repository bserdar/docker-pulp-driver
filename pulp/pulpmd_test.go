package pulp

import (
	"testing"
)

func TestReadJsonFile(t *testing.T) {
	var md repoMd

	_, err := readJsonFile("./testdata/rhel-v2.json", &md)
	if err != nil {
		t.Errorf("Error:%v\n", err)
	}
	if md.RepoId != "rhel7.3" {
		t.Errorf("Unexpected repoid:%s\n", md.RepoId)
	}
	if md.Url != "https://access.redhat.com/webassets/docker/content/dist/rhel/server/7/7Server/x86_64/containers/registry/rhel7.3/" {
		t.Errorf("Unexpected url:%s\n", md.Url)
	}
	if md.Version != 2 {
		t.Errorf("Unexpected version:%d\n", md.Version)
	}

}

func TestReadFilesInDir(t *testing.T) {
	pd, err := readFilesInDir("./testdata", nil)
	if err != nil {
		t.Errorf("Error:%v\n", err)
	}
	md := pd["rhel-v2.json"].repoMd
	if md.RepoId != "rhel7.3" {
		t.Errorf("Unexpected repoid:%s\n", md.RepoId)
	}
	if md.Url != "https://access.redhat.com/webassets/docker/content/dist/rhel/server/7/7Server/x86_64/containers/registry/rhel7.3/" {
		t.Errorf("Unexpected url:%s\n", md.Url)
	}
	if md.Version != 2 {
		t.Errorf("Unexpected version:%d\n", md.Version)
	}
}
