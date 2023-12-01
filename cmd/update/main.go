package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/xoltia/otame"
)

/* Downloads latest source files and generates the otame.sqlite3 database.
 * This is the recommended way to generate the database, and is useful
 * for updating the database, even as it is being used by other readers.
 */

var (
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

	fmt.Println("Downloading VNDB...")
	vndbFS, err := otame.DownloadVNDB(context.Background())

	if err != nil {
		panic(err)
	}

	defer vndbFS.Close()

	vnImagesFile, err := vndbFS.Open("db/images")

	if err != nil {
		panic(err)
	}

	defer vnImagesFile.Close()

	vnImagesDecoder := otame.NewVNDBImageEntryDecoder(vnImagesFile)

	fmt.Println("Replacing VNDB images...")

	if err = otame.ReplaceVNDBImageEntriesFromIterator(vnImagesDecoder); err != nil {
		panic(err)
	}

	vnsFile, err := vndbFS.Open("db/vn")
	defer vnsFile.Close()

	if err != nil {
		panic(err)
	}

	defer vnsFile.Close()

	fmt.Println("Replacing VNDB visual novels...")

	vnsDecoder := otame.NewVNDBVisualNovelEntryDecoder(vnsFile)

	if err = otame.ReplaceVNDBVisualNovelEntriesFromIterator(vnsDecoder); err != nil {
		panic(err)
	}

	vnTitlesFile, err := vndbFS.Open("db/vn_titles")

	if err != nil {
		panic(err)
	}

	defer vnTitlesFile.Close()

	fmt.Println("Replacing VNDB titles...")

	titlesDecoder := otame.NewVNDBTitleEntryDecoder(vnTitlesFile)

	if err = otame.ReplaceVNDBTitleEntriesFromIterator(titlesDecoder); err != nil {
		panic(err)
	}

	aodbFile, err := otame.DownloadAODB(context.Background())

	fmt.Println("Downloading Anime Offline Database...")

	if err != nil {
		panic(err)
	}

	defer aodbFile.Close()

	fmt.Println("Replacing Anime Offline Database...")

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

	fmt.Println("Downloading AniDB...")

	anidbFile, err := otame.DownloadAniDB(context.Background())

	if err != nil {
		panic(err)
	}

	defer anidbFile.Close()

	fmt.Println("Replacing AniDB...")

	anidbDecoder := otame.NewAniDBEntryDecoder(anidbFile)

	if err = otame.ReplaceAniDBEntriesFromIterator(anidbDecoder); err != nil {
		panic(err)
	}
}
