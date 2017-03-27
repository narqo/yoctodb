package yoctodb_test

import (
	"bytes"
	"context"
	"io/ioutil"
	"testing"

	"github.com/narqo/yoctodb"
)

func BenchmarkDB_Count(b *testing.B) {
	dbData, err := ioutil.ReadFile("testdata/index.yocto")
	if err != nil {
		panic(err)
	}
	db, err := yoctodb.ReadVerifyDB(bytes.NewReader(dbData))
	if err != nil {
		panic(err)
	}

	ctx := context.TODO()

	doc2 := &yoctodb.Select{
		// intentionally counting the worth case
		Where: yoctodb.Eq("id", []byte("autoru-xxxxxxxx")),
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c, err := db.Count(ctx, doc2)
		if err != nil {
			b.Fatalf("error %v", err)
		}
		if c != 0 {
			b.Fatalf("bad count %d", c)
		}
	}
}
