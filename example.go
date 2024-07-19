package main

import (
	"fmt"
	"os"

	"lsmtree/db"
	"lsmtree/kv"
)

func main() {
	dbInst := db.Db{}
	dir := fmt.Sprintf("out/")
	os.MkdirAll(dir, 0755)
	dbInst.Init(dir)
	defer dbInst.Shutdown()
	dbInst.SetKv(kv.Kv{"1", []byte("1"), false})

	fmt.Println(dbInst.GetKv("1"))

	dbInst.DeleteKv("1")
	fmt.Println(dbInst.GetKv("1"))

}
