package otame

import (
	"database/sql"
	"fmt"
	"io"
	nurl "net/url"
	"strings"
	"time"
)

var ErrEOF = io.EOF

type RowIterator[T any] interface {
	Next() (T, error)
}

var db *sql.DB

func OpenDB(fileName string) (err error) {
	dsn := fmt.Sprintf("file:%s?_journal_mode=WAL", fileName)
	db, err = sql.Open(driverName, dsn)

	if err != nil {
		return
	}

	err = createUpdatesTable()

	// TODO: metadata table to remember update time,
	// and version of the database schema.
	return
}

// Just in case you want to run custom queries.
func GetDB() *sql.DB {
	return db
}

func CloseDB() (err error) {
	err = db.Close()
	return
}

func createUpdatesTable() (err error) {
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS meta_updates (
			id INTEGER PRIMARY KEY,
			table_name TEXT NOT NULL,
			timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			first_id INTEGER NOT NULL,
			last_id INTEGER NOT NULL,
			dead BOOLEAN NOT NULL DEFAULT FALSE
		);

		CREATE INDEX IF NOT EXISTS meta_updates_table_name_idx ON meta_updates(table_name);
		CREATE INDEX IF NOT EXISTS meta_updates_timestamp_idx ON meta_updates(timestamp);
	`)

	return
}

func insertTableUpdate(tx *sql.Tx, tableName string, lastID int64, firstID int64) (err error) {
	_, err = tx.Exec(`
		INSERT INTO meta_updates (
			table_name,
			last_id,
			first_id
		) VALUES (?, ?, ?)
	`, tableName, lastID, firstID)

	return
}

func getLiveRangeOfTable(tableName string) (firstID int64, lastID int64, err error) {
	query := fmt.Sprintf(`
		SELECT
			COALESCE(meta_updates.first_id, defaults.first_id),
			COALESCE(meta_updates.last_id, defaults.last_id)
		FROM
			(
				SELECT
					MIN(id) AS first_id,
					MAX(id) AS last_id
				FROM %s
			) AS defaults
		LEFT JOIN meta_updates
		ON meta_updates.table_name = '%s'
		AND meta_updates.dead = FALSE
	`, tableName, tableName)

	row := db.QueryRow(query)
	err = row.Scan(&firstID, &lastID)

	return
}

func killAllUpdatesForTable(tx *sql.Tx, tableName string) (err error) {
	_, err = tx.Exec(`
		UPDATE meta_updates
		SET dead = TRUE
		WHERE table_name = ?
	`, tableName)

	return
}

// Will not delete newest update even if it is older than duration.
func ClearUpdatesOlderThan(duration time.Duration) (err error) {
	tablesWithShiftingIDs := []string{
		"anidb_titles",
		"vndb_titles",
		"anime_offline_database",
	}

	seconds := int(duration.Abs().Seconds())

	tx, err := db.Begin()

	defer tx.Rollback()

	// dont kill newest update
	killUpdatesAndReturnRangeStmt, err := tx.Prepare(`
		UPDATE meta_updates
		SET dead = TRUE
		WHERE table_name = ?
		AND CAST(strftime('%s', 'now') AS INTEGER) - CAST(strftime('%s', timestamp) AS INTEGER) > ?
		AND dead = FALSE
		AND id != (
			SELECT id
			FROM meta_updates
			WHERE table_name = ?
			AND dead = FALSE
			ORDER BY timestamp DESC
			LIMIT 1
		)
		RETURNING table_name, first_id, last_id;
	`)

	if err != nil {
		return
	}

	defer killUpdatesAndReturnRangeStmt.Close()

	for _, tableName := range tablesWithShiftingIDs {
		var rows *sql.Rows
		rows, err = killUpdatesAndReturnRangeStmt.Query(tableName, seconds, tableName)

		if err != nil {
			return
		}

		var tableName string
		var firstID int64
		var lastID int64

		for rows.Next() {
			err = rows.Scan(&tableName, &firstID, &lastID)

			if err != nil {
				return
			}

			query := fmt.Sprintf(`
				DELETE FROM %s
				WHERE id BETWEEN ? AND ?
			`, tableName)

			_, err = tx.Exec(query, firstID, lastID)

			if err != nil {
				return
			}
		}
	}

	err = tx.Commit()
	return
}

func CreateAllDBTables() (err error) {
	if err = CreateAnimeOfflineDatabaseTables(); err != nil {
		return
	}

	if err = CreateAniDBTables(); err != nil {
		return
	}

	err = CreateVNDBTables()

	return
}

func NewDBTransaction() (tx *sql.Tx, err error) {
	tx, err = db.Begin()
	return
}

func CreateAnimeOfflineDatabaseTables() (err error) {
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS anime_offline_database (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
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
			anime_offline_database_id INTEGER NOT NULL,
			synonym TEXT NOT NULL,
			FOREIGN KEY(anime_offline_database_id) REFERENCES anime_offline_database(id)
		);

		CREATE INDEX IF NOT EXISTS
			anime_offline_database_synonyms_anime_offline_database_id_idx
		ON
			anime_offline_database_synonyms(anime_offline_database_id);
		
		CREATE TABLE IF NOT EXISTS anime_offline_database_relations (
			anime_offline_database_id INTEGER NOT NULL,
			relation TEXT NOT NULL,
			FOREIGN KEY(anime_offline_database_id) REFERENCES anime_offline_database(id)
		);

		CREATE INDEX IF NOT EXISTS
			anime_offline_database_relations_anime_offline_database_id_idx
		ON
			anime_offline_database_relations(anime_offline_database_id);
		
		CREATE TABLE IF NOT EXISTS anime_offline_database_tags (
			anime_offline_database_id INTEGER NOT NULL,
			tag TEXT NOT NULL,
			FOREIGN KEY(anime_offline_database_id) REFERENCES anime_offline_database(id)
		);

		CREATE INDEX IF NOT EXISTS
			anime_offline_database_tags_anime_offline_database_id_idx
		ON
			anime_offline_database_tags(anime_offline_database_id);
		
		CREATE TABLE IF NOT EXISTS anime_offline_database_sources (
			anime_offline_database_id INTEGER NOT NULL,
			source_name TEXT NOT NULL,
			source_url TEXT NOT NULL,
			source_id TEXT NOT NULL,
			FOREIGN KEY(anime_offline_database_id) REFERENCES anime_offline_database(id)
		);

		CREATE INDEX IF NOT EXISTS
			anime_offline_database_sources_anime_offline_database_id_idx
		ON
			anime_offline_database_sources(anime_offline_database_id);

		CREATE UNIQUE INDEX IF NOT EXISTS
			anime_offline_database_sources_source_id_and_source_name_idx
		ON
			anime_offline_database_sources(source_id, source_name);
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

	if err != nil {
		return
	}

	err = killAllUpdatesForTable(tx, "anime_offline_database")

	return
}

func CreateAnimeOfflineDatabaseEntryWithTx(tx *sql.Tx, entry AnimeOfflineDatabaseEntry) (id int64, err error) {
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
			thumbnail
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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
	)

	if err != nil {
		return
	}

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

func UpdateAnimeOfflineDatabaseEntriesFromIterator[
	T RowIterator[AnimeOfflineDatabaseEntry],
](iter T) (err error) {
	tx, err := db.Begin()

	if err != nil {
		return
	}

	defer tx.Rollback()

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

	if err != nil {
		return
	}

	var entry AnimeOfflineDatabaseEntry
	entry, err = iter.Next()

	if err != nil {
		return
	}

	var lastID int64
	firstID, err := CreateAnimeOfflineDatabaseEntryWithTx(tx, entry)

	if err != nil {
		return
	}

	var id int64
	for {
		entry, err = iter.Next()

		if err == ErrEOF {
			lastID = id
			break
		}

		if err != nil {
			return
		}

		id, err = CreateAnimeOfflineDatabaseEntryWithTx(tx, entry)

		if err != nil {
			return
		}
	}

	err = insertTableUpdate(tx, "anime_offline_database", lastID, firstID)

	if err != nil {
		return
	}

	err = tx.Commit()

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

		if err == ErrEOF {
			break
		}

		if err != nil {
			return
		}

		_, err = CreateAnimeOfflineDatabaseEntryWithTx(tx, entry)

		if err != nil {
			return
		}
	}

	err = tx.Commit()

	return
}

func CreateAniDBTables() (err error) {
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS anidb_titles (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			aid TEXT NOT NULL,
			type TEXT NOT NULL,
			title TEXT NOT NULL,
			language TEXT NOT NULL
		);

		CREATE INDEX IF NOT EXISTS anidb_titles_aid_idx ON anidb_titles(aid);

		CREATE VIRTUAL TABLE IF NOT EXISTS anidb_titles_x_jat_fts_idx USING fts4(
			title,
			content='anidb_titles',
			tokenize='simple'
		);

		CREATE VIRTUAL TABLE IF NOT EXISTS anidb_titles_ja_fts_idx USING fts4(
			title,
			content='anidb_titles',
			tokenize=icu ja
		);

		CREATE VIRTUAL TABLE IF NOT EXISTS anidb_titles_en_fts_idx USING fts4(
			title,
			content='anidb_titles',
			tokenize=icu en
		);

		CREATE TRIGGER IF NOT EXISTS anidb_titles_after_insert_x_jat AFTER INSERT ON anidb_titles
		WHEN new.language = 'x_jat'
		BEGIN
			INSERT INTO anidb_titles_x_jat_fts_idx(docid, title) VALUES (new.id, new.title);
		END;

		CREATE TRIGGER IF NOT EXISTS anidb_titles_after_insert_ja AFTER INSERT ON anidb_titles
		WHEN new.language = 'ja'
		BEGIN
			INSERT INTO anidb_titles_ja_fts_idx(docid, title) VALUES (new.id, new.title);
		END;

		CREATE TRIGGER IF NOT EXISTS anidb_titles_after_insert_en AFTER INSERT ON anidb_titles
		WHEN new.language = 'en'
		BEGIN
			INSERT INTO anidb_titles_en_fts_idx(docid, title) VALUES (new.id, new.title);
		END;

		CREATE TRIGGER IF NOT EXISTS anidb_titles_before_delete_x_jat BEFORE DELETE ON anidb_titles
		WHEN old.language = 'x_jat'
		BEGIN
  			DELETE FROM anidb_titles_x_jat_fts_idx WHERE docid = old.id;
		END;

		CREATE TRIGGER IF NOT EXISTS anidb_titles_before_delete_ja BEFORE DELETE ON anidb_titles
		WHEN old.language = 'ja'
		BEGIN
			DELETE FROM anidb_titles_ja_fts_idx WHERE docid = old.id;
		END;

		CREATE TRIGGER IF NOT EXISTS anidb_titles_before_delete_en BEFORE DELETE ON anidb_titles
		WHEN old.language = 'en'
		BEGIN
			DELETE FROM anidb_titles_en_fts_idx WHERE docid = old.id;
		END;
	`)

	return
}

func DeleteAllAniDBEntriesWithTx(tx *sql.Tx) (err error) {
	_, err = tx.Exec("DELETE FROM anidb_titles")

	if err != nil {
		return
	}

	err = killAllUpdatesForTable(tx, "anidb_titles")

	return
}

func UpdateAniDBEntriesFromIterator[
	T RowIterator[AniDBEntry],
](iter T) (err error) {
	tx, err := db.Begin()

	if err != nil {
		return
	}

	defer tx.Rollback()

	var entry AniDBEntry
	entry, err = iter.Next()

	if err != nil {
		return
	}

	var lastID int64
	firstID, err := CreateAniDBEntryWithTx(tx, entry)

	if err != nil {
		return
	}

	var id int64
	for {
		entry, err = iter.Next()

		if err == ErrEOF {
			lastID = id
			break
		}

		if err != nil {
			return
		}

		id, err = CreateAniDBEntryWithTx(tx, entry)

		if err != nil {
			return
		}
	}

	err = insertTableUpdate(tx, "anidb_titles", lastID, firstID)

	if err != nil {
		return
	}

	err = tx.Commit()

	return
}

func ReplaceAniDBEntriesFromIterator[
	T RowIterator[AniDBEntry],
](iter T) (err error) {
	tx, err := db.Begin()

	if err != nil {
		return
	}

	defer tx.Rollback()

	if err = DeleteAllAniDBEntriesWithTx(tx); err != nil {
		return
	}

	for {
		var entry AniDBEntry
		entry, err = iter.Next()

		if err == ErrEOF {
			break
		}

		if err != nil {
			return
		}

		_, err = CreateAniDBEntryWithTx(tx, entry)

		if err != nil {
			return
		}
	}

	err = tx.Commit()

	return
}

func CreateAniDBEntryWithTx(tx *sql.Tx, entry AniDBEntry) (id int64, err error) {
	var stmt *sql.Stmt
	stmt, err = tx.Prepare(`
		INSERT INTO anidb_titles (
			aid,
			type,
			title,
			language
		) VALUES (?, ?, ?, ?)
	`)

	if err != nil {
		return
	}

	defer stmt.Close()

	result, err := stmt.Exec(
		entry.AID,
		entry.Type,
		entry.Title,
		entry.Language,
	)

	if err == nil {
		id, err = result.LastInsertId()
	}

	return
}

func CreateVNDBTables() (err error) {
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS vndb_visual_novels (
			vnid TEXT PRIMARY KEY NOT NULL,
			original_language TEXT NOT NULL,
			image_id TEXT,
			FOREIGN KEY(image_id) REFERENCES vndb_images(id)
		);

		CREATE TABLE IF NOT EXISTS vndb_titles (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			vnid TEXT NOT NULL,
			title TEXT NOT NULL,
			language TEXT NOT NULL,
			official BOOLEAN NOT NULL,
			latin TEXT,
			FOREIGN KEY(vnid) REFERENCES vndb_visual_novels(vnid)
		);

		CREATE INDEX IF NOT EXISTS vndb_titles_vnid_idx ON vndb_titles(vnid);

		CREATE TABLE IF NOT EXISTS vndb_images (
			id TEXT PRIMARY KEY NOT NULL,
			width INTEGER NOT NULL,
			height INTEGER NOT NULL,
			sexual_avg INTEGER NOT NULL,
			sexual_dev INTEGER NOT NULL,
			violence_avg INTEGER NOT NULL,
			violence_dev INTEGER NOT NULL
		);

		CREATE VIRTUAL TABLE IF NOT EXISTS vndb_titles_ja_fts_idx USING fts4(
			title,
			content='vndb_titles',
			tokenize=icu ja
		);

		CREATE VIRTUAL TABLE IF NOT EXISTS vndb_titles_en_fts_idx USING fts4(
			title,
			content='vndb_titles',
			tokenize=icu en
		);

		CREATE TRIGGER IF NOT EXISTS vndb_titles_after_insert_ja AFTER INSERT ON vndb_titles
		WHEN new.language = 'ja'
		BEGIN
			INSERT INTO vndb_titles_ja_fts_idx(docid, title) VALUES (new.id, new.title);
		END;

		CREATE TRIGGER IF NOT EXISTS vndb_titles_after_insert_en AFTER INSERT ON vndb_titles
		WHEN new.language = 'en'
		BEGIN
			INSERT INTO vndb_titles_en_fts_idx(docid, title) VALUES (new.id, new.title);
		END;

		CREATE TRIGGER IF NOT EXISTS vndb_titles_before_delete_ja BEFORE DELETE ON vndb_titles
		WHEN old.language = 'ja'
		BEGIN
			DELETE FROM vndb_titles_ja_fts_idx WHERE docid = old.id;
		END;

		CREATE TRIGGER IF NOT EXISTS vndb_titles_before_delete_en BEFORE DELETE ON vndb_titles
		WHEN old.language = 'en'
		BEGIN
			DELETE FROM vndb_titles_en_fts_idx WHERE docid = old.id;
		END;
	`)

	return
}

func DeleteAllVNDBVisualNovelEntriesWithTx(tx *sql.Tx) (err error) {
	_, err = tx.Exec("DELETE FROM vndb_visual_novels")

	return
}

func ReplaceVNDBVisualNovelEntriesFromIterator[
	T RowIterator[VNDBVisualNovelEntry],
](iter T) (err error) {
	tx, err := db.Begin()

	if err != nil {
		return
	}

	defer tx.Rollback()

	if err = DeleteAllVNDBVisualNovelEntriesWithTx(tx); err != nil {
		return
	}

	for {
		var entry VNDBVisualNovelEntry
		entry, err = iter.Next()

		if err == ErrEOF {
			break
		}

		if err != nil {
			return
		}

		err = CreateVNDBVisualNovelEntryWithTx(tx, entry)

		if err != nil {
			return
		}
	}

	err = tx.Commit()

	return
}

func CreateVNDBVisualNovelEntryWithTx(tx *sql.Tx, entry VNDBVisualNovelEntry) (err error) {
	var stmt *sql.Stmt

	stmt, err = tx.Prepare(`
		INSERT INTO vndb_visual_novels (
			vnid,
			original_language,
			image_id
		) VALUES (?, ?, ?)
	`)

	if err != nil {
		return
	}

	defer stmt.Close()

	_, err = stmt.Exec(
		entry.ID,
		entry.OriginalLanguage,
		entry.ImageID,
	)

	return
}

func DeleteAllVNDBTitleEntriesWithTx(tx *sql.Tx) (err error) {
	_, err = tx.Exec("DELETE FROM vndb_titles")

	if err != nil {
		return
	}

	err = killAllUpdatesForTable(tx, "vndb_titles")

	return
}

func UpdateVNDBTitleEntriesFromIterator[
	T RowIterator[VNDBTitleEntry],
](iter T) (err error) {
	tx, err := db.Begin()

	if err != nil {
		return
	}

	defer tx.Rollback()

	var entry VNDBTitleEntry
	entry, err = iter.Next()

	if err != nil {
		return
	}

	var lastID int64
	firstID, err := CreateVNDBTitleEntryWithTx(tx, entry)

	if err != nil {
		return
	}

	var id int64
	for {
		entry, err = iter.Next()

		if err == ErrEOF {
			lastID = id
			break
		}

		if err != nil {
			return
		}

		id, err = CreateVNDBTitleEntryWithTx(tx, entry)

		if err != nil {
			return
		}
	}

	err = insertTableUpdate(tx, "vndb_titles", lastID, firstID)

	if err != nil {
		return
	}

	err = tx.Commit()

	return
}

func ReplaceVNDBTitleEntriesFromIterator[
	T RowIterator[VNDBTitleEntry],
](iter T) (err error) {
	tx, err := db.Begin()

	if err != nil {
		return
	}

	defer tx.Rollback()

	if err = DeleteAllVNDBTitleEntriesWithTx(tx); err != nil {
		return
	}

	for {
		var entry VNDBTitleEntry
		entry, err = iter.Next()

		if err == ErrEOF {
			break
		}

		if err != nil {
			return
		}

		_, err = CreateVNDBTitleEntryWithTx(tx, entry)

		if err != nil {
			return
		}
	}

	err = tx.Commit()

	return
}

func CreateVNDBTitleEntryWithTx(tx *sql.Tx, entry VNDBTitleEntry) (id int64, err error) {
	var stmt *sql.Stmt
	stmt, err = tx.Prepare(`
		INSERT INTO vndb_titles (
			vnid,
			title,
			language,
			official,
			latin
		) VALUES (?, ?, ?, ?, ?)
	`)

	if err != nil {
		return
	}

	defer stmt.Close()

	result, err := stmt.Exec(
		entry.VNID,
		entry.Title,
		entry.Language,
		entry.Official,
		entry.Latin,
	)

	if err == nil {
		id, err = result.LastInsertId()
	}

	return
}

func CreateVNDBImageEntryWithTx(tx *sql.Tx, entry VNDBImageEntry) (err error) {
	var stmt *sql.Stmt
	stmt, err = tx.Prepare(`
		INSERT INTO vndb_images (
			id,
			width,
			height,
			sexual_avg,
			sexual_dev,
			violence_avg,
			violence_dev
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`)

	if err != nil {
		return
	}

	defer stmt.Close()

	_, err = stmt.Exec(
		entry.ID,
		entry.Width,
		entry.Height,
		entry.SexualAvg,
		entry.SexualDev,
		entry.ViolenceAvg,
		entry.ViolenceDev,
	)

	return
}

func DeleteAllVNDBImageEntriesWithTx(tx *sql.Tx) (err error) {
	_, err = tx.Exec("DELETE FROM vndb_images")

	return
}

func ReplaceVNDBImageEntriesFromIterator[
	T RowIterator[VNDBImageEntry],
](iter T) (err error) {
	tx, err := db.Begin()

	if err != nil {
		return
	}

	defer tx.Rollback()

	if err = DeleteAllVNDBImageEntriesWithTx(tx); err != nil {
		return
	}

	for {
		var entry VNDBImageEntry
		entry, err = iter.Next()

		if err == ErrEOF {
			break
		}

		if err != nil {
			return
		}

		imgType := entry.ID[0:2]

		// for now we only care about cv images
		if imgType != "cv" {
			continue
		}

		err = CreateVNDBImageEntryWithTx(tx, entry)

		if err != nil {
			return
		}
	}

	err = tx.Commit()

	return
}

// idxTableName should only be used with constant strings of value:
// "anidb_titles_ja_fts_idx", "anidb_titles_en_fts_idx", or "anidb_titles_x_jat_fts_idx"
func searchAniDBTitleIndex(query string, limit int, idxTableName string) (entries []AniDBEntry, err error) {
	firstID, lastID, err := getLiveRangeOfTable("anidb_titles")

	if err != nil {
		return
	}

	// get docids from fts index and join with anidb_titles
	querySQL := fmt.Sprintf(`
		SELECT
			anidb_titles.id,
			anidb_titles.aid,
			anidb_titles.type,
			anidb_titles.title,
			anidb_titles.language
		FROM
			anidb_titles
		JOIN (
			SELECT docid, rank(matchinfo(%s)) AS rank
			FROM %s
			WHERE title MATCH ?
			AND docid BETWEEN ? AND ?
			ORDER BY rank DESC
			LIMIT ?
		) AS matches ON matches.docid = anidb_titles.id
	`, idxTableName, idxTableName)

	rows, err := db.Query(querySQL, query, firstID, lastID, limit)

	if err != nil {
		return
	}

	defer rows.Close()

	for rows.Next() {
		var entry AniDBEntry
		err = rows.Scan(&entry.ID, &entry.AID, &entry.Type, &entry.Title, &entry.Language)

		if err != nil {
			return
		}

		entries = append(entries, entry)
	}

	err = rows.Err()

	return
}

func SearchAniDBJapaneseTitles(query string, limit int) ([]AniDBEntry, error) {
	return searchAniDBTitleIndex(query, limit, "anidb_titles_ja_fts_idx")
}

func SearchAniDBEnglishTitles(query string, limit int) ([]AniDBEntry, error) {
	return searchAniDBTitleIndex(query, limit, "anidb_titles_en_fts_idx")
}

func SearchAniDBRomajiTitles(query string, limit int) ([]AniDBEntry, error) {
	return searchAniDBTitleIndex(query, limit, "anidb_titles_x_jat_fts_idx")
}

// Prefers Japanese titles, then English titles, then romaji titles.
func SearchAniDBTitles(query string, limit int) (entries []AniDBEntry, err error) {
	jaEntries, err := SearchAniDBJapaneseTitles(query, limit)

	if err != nil {
		return
	}

	entries = append(entries, jaEntries...)

	if len(entries) >= limit {
		return
	}

	enEntries, err := SearchAniDBEnglishTitles(query, limit)

	if err != nil {
		return
	}

	if len(entries) >= limit {
		return
	}

	entries = append(entries, enEntries...)

	romajiEntries, err := SearchAniDBRomajiTitles(query, limit)

	if err != nil {
		return
	}

	entries = append(entries, romajiEntries...)

	if len(entries) >= limit {
		entries = entries[:limit]
	}

	return
}

// Can also retrieve dead entries, thus can
// potentially fail to find an entry which was previously
// a valid ID.
func GetAniDBTitleByID(id string) (entry AniDBEntry, err error) {
	row := db.QueryRow(`
		SELECT
			anidb_titles.id,
			anidb_titles.aid,
			anidb_titles.type,
			anidb_titles.title,
			anidb_titles.language
		FROM
			anidb_titles
		WHERE
			anidb_titles.id = ?
	`, id)

	err = row.Scan(&entry.ID, &entry.AID, &entry.Type, &entry.Title, &entry.Language)

	return
}

func GetAniDBTitlesByAID(aid string) (entries []AniDBEntry, err error) {
	firstID, lastID, err := getLiveRangeOfTable("anidb_titles")

	if err != nil {
		return
	}

	rows, err := db.Query(`
		SELECT
			anidb_titles.id,
			anidb_titles.aid,
			anidb_titles.type,
			anidb_titles.title,
			anidb_titles.language
		FROM
			anidb_titles
		WHERE
			anidb_titles.aid = ?
		AND
			anidb_titles.id BETWEEN ? AND ?
	`, aid, firstID, lastID)

	if err != nil {
		return
	}

	defer rows.Close()

	for rows.Next() {
		var entry AniDBEntry
		err = rows.Scan(&entry.ID, &entry.AID, &entry.Type, &entry.Title, &entry.Language)

		if err != nil {
			return
		}

		entries = append(entries, entry)
	}

	err = rows.Err()

	return
}

// Get entries by ID
func GetAnimeOfflineDatabaseEntryByID(id string) (entry AnimeOfflineDatabaseEntry, err error) {
	row := db.QueryRow(`
		SELECT
			anime_offline_database.id,
			anime_offline_database.title,
			anime_offline_database.type,
			anime_offline_database.episodes,
			anime_offline_database.status,
			anime_offline_database.season,
			anime_offline_database.season_year,
			anime_offline_database.picture,
			anime_offline_database.thumbnail
		FROM
			anime_offline_database
		WHERE
			anime_offline_database.id = ?
	`, id)

	err = row.Scan(
		&id,
		&entry.Title,
		&entry.Type,
		&entry.Episodes,
		&entry.Status,
		&entry.AnimeSeason.Season,
		&entry.AnimeSeason.Year,
		&entry.Picture,
		&entry.Thumbnail,
	)

	if err != nil {
		return
	}

	err = appendDetailsToAnimeOfflineDatabaseEntry(id, &entry)

	return
}

func GetAnimeOfflineDatabaseEntryByAID(aid string) (AnimeOfflineDatabaseEntry, error) {
	return GetAnimeOfflineDatabaseEntryBySource("anidb.net", aid)
}

func GetAnimeOfflineDatabaseEntryBySource(sourceName string, sourceID string) (entry AnimeOfflineDatabaseEntry, err error) {
	var id string

	row := db.QueryRow(`
		SELECT
			anime_offline_database.id,
			anime_offline_database.title,
			anime_offline_database.type,
			anime_offline_database.episodes,
			anime_offline_database.status,
			anime_offline_database.season,
			anime_offline_database.season_year,
			anime_offline_database.picture,
			anime_offline_database.thumbnail
		FROM
			anime_offline_database
		JOIN
			anime_offline_database_sources
		ON
			anime_offline_database.id = anime_offline_database_sources.anime_offline_database_id
		WHERE
			anime_offline_database_sources.source_name = ?
		AND
			anime_offline_database_sources.source_id = ?
	`, sourceName, sourceID)

	err = row.Scan(
		&id,
		&entry.Title,
		&entry.Type,
		&entry.Episodes,
		&entry.Status,
		&entry.AnimeSeason.Season,
		&entry.AnimeSeason.Year,
		&entry.Picture,
		&entry.Thumbnail,
	)

	if err != nil {
		return
	}

	err = appendDetailsToAnimeOfflineDatabaseEntry(id, &entry)

	return
}

func appendDetailsToAnimeOfflineDatabaseEntry(aid string, entry *AnimeOfflineDatabaseEntry) (err error) {
	sourcesRows, err := db.Query(`
		SELECT
			anime_offline_database_sources.source_url
		FROM
			anime_offline_database_sources
		WHERE
			anime_offline_database_sources.anime_offline_database_id = ?
	`, aid)

	if err != nil {
		return
	}

	defer sourcesRows.Close()

	for sourcesRows.Next() {
		var source string
		err = sourcesRows.Scan(&source)

		if err != nil {
			return
		}

		entry.Sources = append(entry.Sources, source)
	}

	err = sourcesRows.Err()

	if err != nil {
		return
	}

	synonymsRows, err := db.Query(`
		SELECT
			anime_offline_database_synonyms.synonym
		FROM
			anime_offline_database_synonyms
		WHERE
			anime_offline_database_synonyms.anime_offline_database_id = ?
	`, aid)

	if err != nil {
		return
	}

	defer synonymsRows.Close()

	for synonymsRows.Next() {
		var synonym string
		err = synonymsRows.Scan(&synonym)

		if err != nil {
			return
		}

		entry.Synonyms = append(entry.Synonyms, synonym)
	}

	err = synonymsRows.Err()

	if err != nil {
		return
	}

	relationsRows, err := db.Query(`
		SELECT
			anime_offline_database_relations.relation
		FROM
			anime_offline_database_relations
		WHERE
			anime_offline_database_relations.anime_offline_database_id = ?
	`, aid)

	if err != nil {
		return
	}

	defer relationsRows.Close()

	for relationsRows.Next() {
		var relation string
		err = relationsRows.Scan(&relation)

		if err != nil {
			return
		}

		entry.Relations = append(entry.Relations, relation)
	}

	err = relationsRows.Err()

	if err != nil {
		return
	}

	tagsRows, err := db.Query(`
		SELECT
			anime_offline_database_tags.tag
		FROM
			anime_offline_database_tags
		WHERE
			anime_offline_database_tags.anime_offline_database_id = ?
	`, aid)

	if err != nil {
		return
	}

	defer tagsRows.Close()

	for tagsRows.Next() {
		var tag string
		err = tagsRows.Scan(&tag)

		if err != nil {
			return
		}

		entry.Tags = append(entry.Tags, tag)
	}

	err = tagsRows.Err()

	return
}

func GetVNDBVisualNovelByID(vnid string) (entry VNDBVisualNovelEntry, err error) {
	row := db.QueryRow(`
		SELECT
			vndb_visual_novels.vnid,
			vndb_visual_novels.original_language,
			vndb_visual_novels.image_id
		FROM
			vndb_visual_novels
		WHERE
			vndb_visual_novels.vnid = ?
	`, vnid)

	err = row.Scan(
		&entry.ID,
		&entry.OriginalLanguage,
		&entry.ImageID,
	)

	return
}

func GetVNDBTitlesByVNID(vnid string) (entries []VNDBTitleEntry, err error) {
	firstID, lastID, err := getLiveRangeOfTable("vndb_titles")

	if err != nil {
		return
	}

	rows, err := db.Query(`
		SELECT
			vndb_titles.id,
			vndb_titles.vnid,
			vndb_titles.title,
			vndb_titles.language,
			vndb_titles.official,
			vndb_titles.latin
		FROM
			vndb_titles
		WHERE
			vndb_titles.vnid = ?
		AND
			vndb_titles.id BETWEEN ? AND ?
	`, vnid, firstID, lastID)

	if err != nil {
		return
	}

	defer rows.Close()

	for rows.Next() {
		var entry VNDBTitleEntry
		err = rows.Scan(
			&entry.ID,
			&entry.VNID,
			&entry.Title,
			&entry.Language,
			&entry.Official,
			&entry.Latin,
		)

		if err != nil {
			return
		}

		entries = append(entries, entry)
	}

	err = rows.Err()

	return
}

func GetVNDBImageInfoByID(id string) (entry VNDBImageEntry, err error) {
	row := db.QueryRow(`
		SELECT
			vndb_images.id,
			vndb_images.width,
			vndb_images.height,
			vndb_images.sexual_avg,
			vndb_images.sexual_dev,
			vndb_images.violence_avg,
			vndb_images.violence_dev
		FROM
			vndb_images
		WHERE
			vndb_images.id = ?
	`, id)

	err = row.Scan(
		&entry.ID,
		&entry.Width,
		&entry.Height,
		&entry.SexualAvg,
		&entry.SexualDev,
		&entry.ViolenceAvg,
		&entry.ViolenceDev,
	)

	return
}

func searchVNDBTitleIndex(query string, limit int, idxTableName string) (entries []VNDBTitleEntry, err error) {
	firstID, lastID, err := getLiveRangeOfTable("vndb_titles")

	if err != nil {
		return
	}

	querySQL := fmt.Sprintf(`
		SELECT
			vndb_titles.id,
			vndb_titles.vnid,
			vndb_titles.title,
			vndb_titles.language,
			vndb_titles.official,
			vndb_titles.latin
		FROM
			vndb_titles
		JOIN (
			SELECT docid, rank(matchinfo(%s)) AS rank
			FROM %s
			WHERE title MATCH ?
			AND docid BETWEEN ? AND ?
			ORDER BY rank DESC
			LIMIT ?
		) AS matches ON matches.docid = vndb_titles.id
	`, idxTableName, idxTableName)

	rows, err := db.Query(querySQL, query, firstID, lastID, limit)

	if err != nil {
		return
	}

	defer rows.Close()

	for rows.Next() {
		var entry VNDBTitleEntry
		err = rows.Scan(
			&entry.ID,
			&entry.VNID,
			&entry.Title,
			&entry.Language,
			&entry.Official,
			&entry.Latin,
		)

		if err != nil {
			return
		}

		entries = append(entries, entry)
	}

	err = rows.Err()

	return
}

func SearchVNDBJapaneseTitles(query string, limit int) ([]VNDBTitleEntry, error) {
	return searchVNDBTitleIndex(query, limit, "vndb_titles_ja_fts_idx")
}

func SearchVNDBEnglishTitles(query string, limit int) ([]VNDBTitleEntry, error) {
	return searchVNDBTitleIndex(query, limit, "vndb_titles_en_fts_idx")
}

func SearchVNDBTitles(query string, limit int) (entries []VNDBTitleEntry, err error) {
	jaEntries, err := SearchVNDBJapaneseTitles(query, limit)

	if err != nil {
		return
	}

	entries = append(entries, jaEntries...)

	if len(entries) >= limit {
		return
	}

	enEntries, err := SearchVNDBEnglishTitles(query, limit)

	if err != nil {
		return
	}

	if len(entries) >= limit {
		return
	}

	entries = append(entries, enEntries...)

	if len(entries) >= limit {
		entries = entries[:limit]
	}

	return
}
