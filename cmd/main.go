package main

import (
	"flag"
	"os"

	_ "github.com/mattn/go-sqlite3"
	"github.com/xoltia/otame"
)

var (
	aodbPath   = flag.String("aodb", "./data/anime-offline-database-minified.json", "Path to anime-offline-database.json")
	anidbPath  = flag.String("anidb", "./data/anidb-titles.dat", "Path to anidb-titles.dat")
	outputPath = flag.String("o", "./otame.sqlite3", "Path to output sqlite3 database")
)

func main() {
	flag.Parse()

	aodbFile, err := os.Open(*aodbPath)

	if err != nil {
		panic(err)
	}

	defer aodbFile.Close()

	aodbDecoder := otame.NewAnimeOfflineDatabaseDecoder(aodbFile)

	err = otame.OpenDB(*outputPath)

	if err != nil {
		panic(err)
	}

	defer otame.CloseDB()

	if err = otame.CreateAnimeOfflineDatabaseTables(); err != nil {
		panic(err)
	}

	if err = otame.ReplaceAnimeOfflineDatabaseEntriesFromIterator(aodbDecoder); err != nil {
		panic(err)
	}

	if err = otame.CreateAniDBTables(); err != nil {
		panic(err)
	}

	anidbFile, err := os.Open(*anidbPath)

	if err != nil {
		panic(err)
	}

	defer anidbFile.Close()

	anidbDecoder := otame.NewAniDBEntryDecoder(anidbFile)

	if err = otame.InsertManyAniDBEntriesFromIterator(anidbDecoder); err != nil {
		panic(err)
	}

}
