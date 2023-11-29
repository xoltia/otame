package main

import (
	"database/sql"
	"io"
	nurl "net/url"
	"os"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"github.com/xoltia/otame"
)

const createAnimeOfflineDatabaseTablesSQL = `
CREATE TABLE IF NOT EXISTS anime_offline_database (
	id INTEGER PRIMARY KEY,
	title TEXT NOT NULL,
	type TEXT NOT NULL,
	episodes INTEGER NOT NULL,
	status TEXT NOT NULL,
	season TEXT NOT NULL,
	season_year INTEGER,
	picture TEXT NOT NULL,
	thumbnail TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS anime_offline_database_synonyms (
	id INTEGER PRIMARY KEY,
	anime_offline_database_id INTEGER NOT NULL,
	synonym TEXT NOT NULL,
	FOREIGN KEY(anime_offline_database_id) REFERENCES anime_offline_database(id)
);

CREATE TABLE IF NOT EXISTS anime_offline_database_relations (
	id INTEGER PRIMARY KEY,
	anime_offline_database_id INTEGER NOT NULL,
	relation TEXT NOT NULL,
	FOREIGN KEY(anime_offline_database_id) REFERENCES anime_offline_database(id)
);

CREATE TABLE IF NOT EXISTS anime_offline_database_tags (
	id INTEGER PRIMARY KEY,
	anime_offline_database_id INTEGER NOT NULL,
	tag TEXT NOT NULL,
	FOREIGN KEY(anime_offline_database_id) REFERENCES anime_offline_database(id)
);

CREATE TABLE IF NOT EXISTS anime_offline_database_sources (
	id INTEGER PRIMARY KEY,
	anime_offline_database_id INTEGER NOT NULL,
	source_name TEXT NOT NULL,
	source_url TEXT NOT NULL,
	source_id TEXT NOT NULL,
	FOREIGN KEY(anime_offline_database_id) REFERENCES anime_offline_database(id)
);
`

func main() {
	db, err := sql.Open("sqlite3", "file:anime-offline-database.sqlite3")

	if err != nil {
		panic(err)
	}

	defer db.Close()

	_, err = db.Exec(createAnimeOfflineDatabaseTablesSQL)

	if err != nil {
		panic(err)
	}

	file, err := os.Open("anime-offline-database.json")

	if err != nil {
		panic(err)
	}

	decoder := otame.NewAnimeOfflineDatabaseDecoder(file)

	tx, err := db.Begin()

	for {
		entry, err := decoder.Next()

		if err == io.EOF {
			break
		}

		if err != nil {
			panic(err)
		}

		if err != nil {
			panic(err)
		}

		stmt, err := tx.Prepare(`
			INSERT INTO anime_offline_database (
				title,
				type,
				episodes,
				status,
				season,
				season_year,
				picture,
				thumbnail
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`)

		if err != nil {
			panic(err)
		}

		defer stmt.Close()

		result, err := stmt.Exec(
			entry.Title,
			entry.Type,
			entry.Episodes,
			entry.Status,
			entry.AnimeSeason.Season,
			entry.AnimeSeason.Year,
			entry.Picture,
			entry.Thumbnail,
		)

		if err != nil {
			panic(err)
		}

		id, err := result.LastInsertId()

		if err != nil {
			panic(err)
		}

		stmt, err = tx.Prepare(`
			INSERT INTO anime_offline_database_synonyms (
				anime_offline_database_id,
				synonym
			) VALUES (?, ?)
		`)

		if err != nil {
			panic(err)
		}

		defer stmt.Close()

		for _, synonym := range entry.Synonyms {

			_, err = stmt.Exec(id, synonym)

			if err != nil {
				panic(err)
			}
		}

		stmt, err = tx.Prepare(`
			INSERT INTO anime_offline_database_relations (
				anime_offline_database_id,
				relation
			) VALUES (?, ?)
		`)

		if err != nil {
			panic(err)
		}

		defer stmt.Close()

		for _, relation := range entry.Relations {
			_, err = stmt.Exec(id, relation)

			if err != nil {
				panic(err)
			}
		}

		stmt, err = tx.Prepare(`
			INSERT INTO anime_offline_database_tags (
				anime_offline_database_id,
				tag
			) VALUES (?, ?)
		`)

		if err != nil {
			panic(err)
		}

		defer stmt.Close()

		for _, tag := range entry.Tags {
			_, err = stmt.Exec(id, tag)

			if err != nil {
				panic(err)
			}
		}

		stmt, err = tx.Prepare(`
			INSERT INTO anime_offline_database_sources (
				anime_offline_database_id,
				source_name,
				source_url,
				source_id
			) VALUES (?, ?, ?, ?)
		`)

		if err != nil {
			panic(err)
		}

		defer stmt.Close()

		for _, source := range entry.Sources {
			url, err := nurl.Parse(source)

			if err != nil {
				panic(err)
			}

			// last part of the path
			pathParts := strings.Split(url.Path, "/")
			sourceID := pathParts[len(pathParts)-1]

			_, err = stmt.Exec(id, url.Hostname(), source, sourceID)

			if err != nil {
				panic(err)
			}
		}
	}

	err = tx.Commit()

	if err != nil {
		panic(err)
	}
}
