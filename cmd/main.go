package main

import (
	"os"

	_ "github.com/mattn/go-sqlite3"
	"github.com/xoltia/otame"
)

func main() {
	decoder := otame.NewAnimeOfflineDatabaseDecoder(os.Stdin)
	err := otame.OpenDB("otame.sqlite3")
	if err != nil {
		panic(err)
	}

	defer otame.CloseDB()

	if err = otame.CreateAnimeOfflineDatabaseTables(); err != nil {
		panic(err)
	}

	if err = otame.ReplaceAnimeOfflineDatabaseEntriesFromIterator(decoder); err != nil {
		panic(err)
	}
}
