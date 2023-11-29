package otame

import (
	"database/sql"
	"fmt"
	"io"
	nurl "net/url"
	"strings"
	"time"
)

type RowIterator[T any] interface {
	Next() (T, error)
}

const driverName = "sqlite3"

var db *sql.DB

func OpenDB(fileName string) (err error) {
	dsn := fmt.Sprintf("file:%s?_journal_mode=WAL", fileName)
	db, err = sql.Open(driverName, dsn)
	return
}

func CloseDB() (err error) {
	err = db.Close()
	return
}

func CreateAnimeOfflineDatabaseTables() (err error) {
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS anime_offline_database (
			id INTEGER PRIMARY KEY,
			title TEXT NOT NULL,
			type TEXT NOT NULL,
			episodes INTEGER NOT NULL,
			status TEXT NOT NULL,
			season TEXT NOT NULL,
			season_year INTEGER,
			picture TEXT NOT NULL,
			thumbnail TEXT NOT NULL,
			inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
	`)
	return
}

func DeleteAllAnimeOfflineDatabaseEntriesWithTx(tx *sql.Tx) (err error) {
	_, err = tx.Exec("DELETE FROM anime_offline_database")

	if err != nil {
		return
	}

	_, err = tx.Exec("DELETE FROM anime_offline_database_synonyms")

	if err != nil {
		return
	}

	_, err = tx.Exec("DELETE FROM anime_offline_database_relations")

	if err != nil {
		return
	}

	_, err = tx.Exec("DELETE FROM anime_offline_database_tags")

	if err != nil {
		return
	}

	_, err = tx.Exec("DELETE FROM anime_offline_database_sources")

	return
}

func CreateAnimeOfflineDatabaseEntryWithTx(tx *sql.Tx, entry AnimeOfflineDatabaseEntry) (err error) {
	var stmt *sql.Stmt
	stmt, err = tx.Prepare(`
		INSERT INTO anime_offline_database (
			title,
			type,
			episodes,
			status,
			season,
			season_year,
			picture,
			thumbnail,
			inserted_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)

	if err != nil {
		return
	}

	defer stmt.Close()

	var result sql.Result
	result, err = stmt.Exec(
		entry.Title,
		entry.Type,
		entry.Episodes,
		entry.Status,
		entry.AnimeSeason.Season,
		entry.AnimeSeason.Year,
		entry.Picture,
		entry.Thumbnail,
		time.Now(),
	)

	if err != nil {
		return
	}

	var id int64
	id, err = result.LastInsertId()

	if err != nil {
		return
	}

	stmt, err = tx.Prepare(`
		INSERT INTO anime_offline_database_synonyms (
			anime_offline_database_id,
			synonym
		) VALUES (?, ?)
	`)

	if err != nil {
		return
	}

	defer stmt.Close()

	for _, synonym := range entry.Synonyms {

		_, err = stmt.Exec(id, synonym)

		if err != nil {
			return
		}
	}

	stmt, err = tx.Prepare(`
		INSERT INTO anime_offline_database_relations (
			anime_offline_database_id,
			relation
		) VALUES (?, ?)
	`)

	if err != nil {
		return
	}

	defer stmt.Close()

	for _, relation := range entry.Relations {
		_, err = stmt.Exec(id, relation)

		if err != nil {
			return
		}
	}

	stmt, err = tx.Prepare(`
		INSERT INTO anime_offline_database_tags (
			anime_offline_database_id,
			tag
		) VALUES (?, ?)
	`)

	if err != nil {
		return
	}

	defer stmt.Close()

	for _, tag := range entry.Tags {
		_, err = stmt.Exec(id, tag)

		if err != nil {
			return
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
		return
	}

	defer stmt.Close()

	for _, source := range entry.Sources {
		var url *nurl.URL
		url, err = nurl.Parse(source)

		if err != nil {
			return
		}

		sourceID := url.Path[strings.LastIndex(url.Path, "/")+1:]

		_, err = stmt.Exec(id, url.Hostname(), source, sourceID)

		if err != nil {
			return
		}
	}

	return
}

func ReplaceAnimeOfflineDatabaseEntriesFromIterator[
	T RowIterator[AnimeOfflineDatabaseEntry],
](iter T) (err error) {
	tx, err := db.Begin()

	if err != nil {
		return
	}

	defer tx.Rollback()

	err = DeleteAllAnimeOfflineDatabaseEntriesWithTx(tx)

	if err != nil {
		return
	}

	for {
		var entry AnimeOfflineDatabaseEntry
		entry, err = iter.Next()

		if err == io.EOF {
			break
		}

		if err != nil {
			return
		}

		err = CreateAnimeOfflineDatabaseEntryWithTx(tx, entry)

		if err != nil {
			return
		}
	}

	err = tx.Commit()

	return
}