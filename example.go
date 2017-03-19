// +build ignore

package main

import (
	"bytes"
	"io/ioutil"

	"github.com/narqo/yoctodb"
)

func main() {
	dbData, err := ioutil.ReadFile("testdata/index.yocto")
	if err != nil {
		panic(err)
	}
	if _, err := yoctodb.ReadVerifyDB(bytes.NewReader(dbData)); err != nil {
		panic(err)
	}
}
