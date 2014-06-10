package leveldb

import (
	"bytes"
	"sync"
	"testing"
)

var testDB *LevelDBEngine
var onlyOnce sync.Once

func createDB(name string) *LevelDBEngine  {
	f := func() {
		var err error
		testDB, err = Open(name)
		if err != nil  {
			println(err.Error())
			panic(err)
		}
	}
	onlyOnce.Do(f)
	return testDB
}

func TestSimple(t *testing.T)  {
	db := createDB("/tmp/testdb")

	key := []byte("hi")
	value := []byte("hello world")

	if err := db.Put(key, value); err != nil {
		t.Fatal(err)
	}

	if v, err := db.Get(key); err != nil {
		t.Fatal(err)
	} else if (!bytes.Equal(v, value)) {
		t.Fatal("get value not equal")
	}

	if err := db.Delete(key); err != nil {
		t.Fatal(err)
	}

	if v, err := db.Get(key); err != nil {
		t.Fatal(err)
	} else if v != nil {
		t.Fatal("after delete, key should not exist any more")
	}
}	