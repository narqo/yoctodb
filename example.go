// +build ignore

package main

import (
	"bytes"
	"fmt"
	"io/ioutil"

	"github.com/narqo/yoctodb"
)

func main() {
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
	doc2 := &yoctodb.Select{
		Where: &yoctodb.Eq{"id": 2},
	}
	n, err := db.Count(doc2)
	if err != nil {
		panic(err)
	}
	fmt.Fprintf("doc count: %d\n", n)

	/*
	// Filter and sort
	final Query sorted =
                select().where(and(gte("id", from(1)), lte("id", from(2))))
                        .orderBy(desc("score"));
	db.execute(sorted, ...)
	 */
	sorted := &yoctodb.Select{
		Where: &yoctodb.And{
			&yoctodb.Gte{"id": 1},
			&yoctodb.Lte{"id": 1},
		},
		OrderBy: &yoctodb.Desc("score"),
	}
	//err := db.DoProcess(sorted, func() error {})
	err := db.Do(sorted)
}
