package otame

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"strings"
)

// TODO: Add support for other title types.
type AniDBEntry struct {
	ID                    string
	PrimaryTitle          string
	RomajiOfficialTitle   string
	JapaneseOfficialTitle string
	EnglishOfficialTitle  string
	RomajiSynonyms        []string
	JapaneseSynonyms      []string
	EnglishSynonyms       []string
}

type AniDBEntryDecoder struct {
	scanner      *bufio.Scanner
	bufferedLine []string
}

// Warning: This only works if the file is sorted!
// For unsorted data, use the ReadUnsortedAniDBDump function.
func NewAniDBEntryDecoder(r io.Reader) *AniDBEntryDecoder {
	return &AniDBEntryDecoder{
		scanner: bufio.NewScanner(r),
	}
}

// Returns slice of length 4 (aid, titleType, language, title)
func (a *AniDBEntryDecoder) readLine() (line []string, err error) {
	if a.bufferedLine != nil {
		line = a.bufferedLine
		a.bufferedLine = nil
		return
	}

	if !a.scanner.Scan() {
		err = io.EOF
		return
	}

	lineText := a.scanner.Text()

	if lineText == "" || strings.HasPrefix(lineText, "#") {
		line, err = a.readLine()
		return
	}

	line = strings.SplitN(lineText, "|", 4)

	if len(line) != 4 {
		err = fmt.Errorf("invalid line: %s", line)
		return
	}

	err = a.scanner.Err()

	return
}

func appendLineToEntry(entry *AniDBEntry, line []string) {
	aid := line[0]
	titleType := line[1]
	language := line[2]
	title := line[3]

	if entry.ID == "" {
		entry.ID = aid
	}

	switch titleType {
	case "1":
		entry.PrimaryTitle = title
	case "4":
		switch language {
		case "x-jat":
			entry.RomajiOfficialTitle = title
		case "ja":
			entry.JapaneseOfficialTitle = title
		case "en":
			entry.EnglishOfficialTitle = title
		}
	case "2":
		fallthrough
	case "3":
		switch language {
		case "x-jat":
			entry.RomajiSynonyms = append(entry.RomajiSynonyms, title)
		case "ja":
			entry.JapaneseSynonyms = append(entry.JapaneseSynonyms, title)
		case "en":
			entry.EnglishSynonyms = append(entry.EnglishSynonyms, title)
		}
	}
}

func (a *AniDBEntryDecoder) Next() (entry AniDBEntry, err error) {
	var line []string

	for {
		line, err = a.readLine()

		if err != nil {
			return
		}

		aid := line[0]

		if entry.ID == "" {
			entry.ID = aid
		} else if entry.ID != aid {
			a.bufferedLine = line
			return
		}

		appendLineToEntry(&entry, line)
	}
}

func (a *AniDBEntryDecoder) DecodeAll() (entries []AniDBEntry, err error) {
	for {
		entry, err := a.Next()

		if err == io.EOF {
			err = nil
			break
		}

		if err != nil {
			return nil, err
		}

		entries = append(entries, entry)
	}

	return
}

// Returns a map of aid to AniDBEntry.
// Does not require the file to be sorted.
func ReadAniDBDump(r io.Reader) (entries map[string]AniDBEntry, err error) {
	scanner := bufio.NewScanner(r)

	for scanner.Scan() {
		line := scanner.Text()

		if len(line) == 0 {
			continue
		}

		if line[0] == '#' {
			continue
		}

		cols := strings.SplitN(line, "|", 4)

		if len(cols) != 4 {
			log.Println("[Warning] Invalid line:", line)
			continue
		}

		aid := cols[0]
		titleType := cols[1]
		language := cols[2]
		title := cols[3]

		if _, ok := entries[aid]; !ok {
			entries[aid] = AniDBEntry{
				ID:               aid,
				RomajiSynonyms:   []string{},
				JapaneseSynonyms: []string{},
				EnglishSynonyms:  []string{},
			}
		}

		entry := entries[aid]

		switch titleType {
		case "1":
			entry.PrimaryTitle = title
		case "4":
			switch language {
			case "x-jat":
				entry.RomajiOfficialTitle = title
			case "ja":
				entry.JapaneseOfficialTitle = title
			case "en":
				entry.EnglishOfficialTitle = title
			}
		case "2":
			fallthrough
		case "3":
			switch language {
			case "x-jat":
				entry.RomajiSynonyms = append(entry.RomajiSynonyms, title)
			case "ja":
				entry.JapaneseSynonyms = append(entry.JapaneseSynonyms, title)
			case "en":
				entry.EnglishSynonyms = append(entry.EnglishSynonyms, title)
			}
		}

		entries[aid] = entry
	}

	if err = scanner.Err(); err != nil {
		return
	}

	return
}
