package otame

import (
	"database/sql"
	"fmt"
	"io"
	nurl "net/url"
	"strings"
)

var ErrEOF = io.EOF

type RowIterator[T any] interface {
	Next() (T, error)
}

var db *sql.DB

func OpenDB(fileName string) (err error) {
	dsn := fmt.Sprintf("file:%s?_journal_mode=WAL", fileName)
	db, err = sql.Open(driverName, dsn)

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

		CREATE INDEX IF NOT EXISTS
			anime_offline_database_synonyms_anime_offline_database_id_idx
		ON
			anime_offline_database_synonyms(anime_offline_database_id);
		
		CREATE TABLE IF NOT EXISTS anime_offline_database_relations (
			id INTEGER PRIMARY KEY,
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
			id INTEGER PRIMARY KEY,
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

		if err == ErrEOF {
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

func CreateAniDBTables() (err error) {
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS anidb_titles (
			id INTEGER PRIMARY KEY,
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

		err = CreateAniDBEntryWithTx(tx, entry)

		if err != nil {
			return
		}
	}

	err = tx.Commit()

	return
}

func CreateAniDBEntryWithTx(tx *sql.Tx, entry AniDBEntry) (err error) {
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

	_, err = stmt.Exec(
		entry.AID,
		entry.Type,
		entry.Title,
		entry.Language,
	)

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
			id INTEGER PRIMARY KEY,
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

		err = CreateVNDBTitleEntryWithTx(tx, entry)

		if err != nil {
			return
		}
	}

	err = tx.Commit()

	return
}

func CreateVNDBTitleEntryWithTx(tx *sql.Tx, entry VNDBTitleEntry) (err error) {
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

	_, err = stmt.Exec(
		entry.ID,
		entry.Title,
		entry.Language,
		entry.Official,
		entry.Latin,
	)

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
			ORDER BY rank DESC
			LIMIT ?
		) AS matches ON matches.docid = anidb_titles.id
	`, idxTableName, idxTableName)

	rows, err := db.Query(querySQL, query, limit)

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
	`, aid)

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
	rows, err := db.Query(`
		SELECT
			vndb_titles.vnid,
			vndb_titles.title,
			vndb_titles.language,
			vndb_titles.official,
			vndb_titles.latin
		FROM
			vndb_titles
		WHERE
			vndb_titles.vnid = ?
	`, vnid)

	if err != nil {
		return
	}

	defer rows.Close()

	for rows.Next() {
		var entry VNDBTitleEntry
		err = rows.Scan(
			&entry.ID,
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
	querySQL := fmt.Sprintf(`
		SELECT
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
			ORDER BY rank DESC
			LIMIT ?
		) AS matches ON matches.docid = vndb_titles.id
	`, idxTableName, idxTableName)

	rows, err := db.Query(querySQL, query, limit)

	if err != nil {
		return
	}

	defer rows.Close()

	for rows.Next() {
		var entry VNDBTitleEntry
		err = rows.Scan(
			&entry.ID,
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
