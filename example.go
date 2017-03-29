// +build ignore

package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"

	"github.com/narqo/yoctodb"
)

func main() {
	ctx := context.Background()

	dbData, err := ioutil.ReadFile("testdata/index.yocto")
	if err != nil {
		panic(err)
	}
	db, err := yoctodb.ReadVerifyDB(bytes.NewReader(dbData))
	if err != nil {
		panic(err)
	}

	/*
	// Filter the second document
	final Query doc2 = select().where(eq("id", from(2)));
	assertTrue(db.count(doc2) == 1);
	 */

	query := &yoctodb.Select{
		Where: yoctodb.Eq("color", []byte("FF0000")),
	}
	n, err := db.Count(ctx, query)
	if err != nil {
		panic(err)
	}
	fmt.Printf("doc count: %d\n", n)

	/*
	// Filter and sort
	final Query sorted =
                select().where(and(gte("id", from(1)), lte("id", from(2))))
                        .orderBy(desc("score"));
	db.execute(sorted, ...)
	 */
	sorted := &yoctodb.Select{
		Where: yoctodb.And(
			yoctodb.Eq("wheel_key", []byte("LEFT")),
			//yoctodb.Gte{"id": 1},
			//yoctodb.Lte{"id": 1},
		),
		Offset: 1,
		//OrderBy: &yoctodb.Desc("score"),
	}
	docs, err := db.Query(ctx, sorted)
	if err != nil {
		panic(err)
	}
	defer docs.Close()
	for docs.Next() {
		if err := docs.Scan(); err != nil {
			fmt.Printf("error: %v", err)
		}
	}
}
