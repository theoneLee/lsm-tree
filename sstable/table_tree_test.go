package sstable

import (
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_getSstPathList2(t *testing.T) {
	dir := fmt.Sprintf("out/sst/path/%v", time.Now().Unix())
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		panic(err)
	}

	fileList := []string{
		"1.2.db",
		"3.1.db",
		"1.1.db",
		"3.4.db",
		"2.1.db",
	}
	// 构造数据
	for _, name := range fileList {
		os.Create(path.Join(dir, name))
	}

	expectFileList := []string{
		path.Join(dir, "1.1.db"),
		path.Join(dir, "1.2.db"),
		path.Join(dir, "2.1.db"),
		path.Join(dir, "3.1.db"),
		path.Join(dir, "3.4.db"),
	}

	res := getSstPathList2(dir)
	assert.Equal(t, expectFileList, res)

}

func Test_getSstPathList(t *testing.T) {
	dir := fmt.Sprintf("out/sst/path/%v", time.Now().Unix())
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		panic(err)
	}

	fileList := []string{
		"1.2.db",
		"3.1.db",
		"1.1.db",
		"3.4.db",
		"2.1.db",
	}
	// 构造数据
	for _, name := range fileList {
		os.Create(path.Join(dir, name))
	}

	expectFileList := []string{
		path.Join(dir, "1.1.db"),
		path.Join(dir, "1.2.db"),
		path.Join(dir, "2.1.db"),
		path.Join(dir, "3.1.db"),
		path.Join(dir, "3.4.db"),
	}

	res := getSstPathList(dir)
	assert.Equal(t, expectFileList, res)

}
