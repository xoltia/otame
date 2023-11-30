package main

import (
	"flag"
	"os"
	"path"

	"github.com/xoltia/otame"
)

var (
	aodbPath   = flag.String("aodb", "./data/anime-offline-database-minified.json", "Path to anime-offline-database.json")
	anidbPath  = flag.String("anidb", "./data/anidb-titles.dat", "Path to anidb-titles.dat")
	vndbPath   = flag.String("vndb", "./data/vndb-db-latest", "Path to vndb-db-latest (directory)")
	outputPath = flag.String("o", "./otame.sqlite3", "Path to output sqlite3 database")
)

func main() {
	flag.Parse()

	err := otame.OpenDB(*outputPath)

	if err != nil {
		panic(err)
	}

	defer otame.CloseDB()

	if err = otame.CreateVNDBTables(); err != nil {
		panic(err)
	}

	vnImagesPath := path.Join(*vndbPath, "db", "images")
	vnImagesFile, err := os.Open(vnImagesPath)

	if err != nil {
		panic(err)
	}

	defer vnImagesFile.Close()

	vnImagesDecoder := otame.NewVNDBImageEntryDecoder(vnImagesFile)

	if err = otame.ReplaceVNDBImageEntriesFromIterator(vnImagesDecoder); err != nil {
		panic(err)
	}

	vnsFilePath := path.Join(*vndbPath, "db", "vn")
	vnsFile, err := os.Open(vnsFilePath)

	if err != nil {
		panic(err)
	}

	defer vnsFile.Close()

	vnsDecoder := otame.NewVNDBVisualNovelEntryDecoder(vnsFile)

	if err = otame.ReplaceVNDBVisualNovelEntriesFromIterator(vnsDecoder); err != nil {
		panic(err)
	}

	vnTitlesFilePath := path.Join(*vndbPath, "db", "vn_titles")
	vnTitlesFile, err := os.Open(vnTitlesFilePath)

	if err != nil {
		panic(err)
	}

	defer vnTitlesFile.Close()

	titlesDecoder := otame.NewVNDBTitleEntryDecoder(vnTitlesFile)

	if err = otame.ReplaceVNDBTitleEntriesFromIterator(titlesDecoder); err != nil {
		panic(err)
	}

	aodbFile, err := os.Open(*aodbPath)

	if err != nil {
		panic(err)
	}

	defer aodbFile.Close()

	aodbDecoder := otame.NewAnimeOfflineDatabaseDecoder(aodbFile)

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

	if err = otame.ReplaceAniDBEntriesFromIterator(anidbDecoder); err != nil {
		panic(err)
	}

}
