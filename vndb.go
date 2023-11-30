package otame

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode"
)

type VNDBTitleEntry struct {
	ID       string
	Language string
	Official bool
	Title    string
	Latin    *string
}

type VNDBVisualNovelEntry struct {
	ID               string
	OriginalLanguage string
	ImageID          *string
}

type VNDBImageEntry struct {
	ID          string
	Width       int
	Height      int
	SexualAvg   int
	SexualDev   int
	ViolenceAvg int
	ViolenceDev int
}

func NewVNDBImageEntryDecoder(r io.Reader) *genericLineDecoder[VNDBImageEntry] {
	// id	width	height	c_votecount	c_sexual_avg	c_sexual_stddev	c_violence_avg	c_violence_stddev	c_weight
	return &genericLineDecoder[VNDBImageEntry]{
		scanner:       bufio.NewScanner(r),
		separatorChar: "\t",
		nCols:         9,
		unmarshal: func(line []string) (entry VNDBImageEntry, err error) {
			entry.ID = line[0]

			if entry.Width, err = strconv.Atoi(line[1]); err != nil {
				err = fmt.Errorf("invalid width: %w", err)
				return
			}

			if entry.Height, err = strconv.Atoi(line[2]); err != nil {
				err = fmt.Errorf("invalid height: %w", err)
				return
			}

			if entry.SexualAvg, err = strconv.Atoi(line[4]); err != nil {
				err = fmt.Errorf("invalid sexual avg: %w", err)
				return
			}

			if entry.SexualDev, err = strconv.Atoi(line[5]); err != nil {
				err = fmt.Errorf("invalid sexual dev: %w", err)
				return
			}

			if entry.ViolenceAvg, err = strconv.Atoi(line[6]); err != nil {
				err = fmt.Errorf("invalid violence avg: %w", err)
				return
			}

			return
		},
	}
}

func NewVNDBTitleEntryDecoder(r io.Reader) *genericLineDecoder[VNDBTitleEntry] {
	return &genericLineDecoder[VNDBTitleEntry]{
		scanner:       bufio.NewScanner(r),
		separatorChar: "\t",
		nCols:         5,
		unmarshal: func(line []string) (entry VNDBTitleEntry, err error) {
			entry.ID = line[0]
			entry.Language = line[1]
			entry.Official = line[2] == "t"
			entry.Title = line[3]
			if line[4] != "\\N" {
				entry.Latin = &line[4]
			}

			return
		},
	}
}

func NewVNDBVisualNovelEntryDecoder(r io.Reader) *genericLineDecoder[VNDBVisualNovelEntry] {
	return &genericLineDecoder[VNDBVisualNovelEntry]{
		scanner:       bufio.NewScanner(r),
		separatorChar: "\t",
		// actually more than 4 columns, but we only care about the first 3
		nCols: 4,
		unmarshal: func(line []string) (entry VNDBVisualNovelEntry, err error) {
			entry.ID = line[0]
			entry.OriginalLanguage = line[1]

			if entry.OriginalLanguage != "\\N" {
				entry.ImageID = &line[2]
			}

			return
		},
	}
}

type genericLineDecoder[T any] struct {
	line          int
	scanner       *bufio.Scanner
	commentChar   string
	separatorChar string
	nCols         int
	unmarshal     func([]string) (T, error)
}

func (d *genericLineDecoder[T]) readLine() (cols []string, err error) {
	if !d.scanner.Scan() {
		err = ErrEOF
		return
	}

	lineText := d.scanner.Text()
	d.line++

	// ignore blank lines and comments
	trimmedLineText := strings.TrimLeftFunc(lineText, unicode.IsSpace)
	isComment := strings.HasPrefix(trimmedLineText, d.commentChar)

	if lineText == "" || isComment && len(d.commentChar) > 0 {
		cols, err = d.readLine()
		return
	}

	cols = strings.SplitN(lineText, d.separatorChar, d.nCols)

	if len(cols) != d.nCols {
		err = fmt.Errorf("invalid line: %s", cols)
	}

	return
}

func (d *genericLineDecoder[T]) Next() (entry T, err error) {
	var line []string

	line, err = d.readLine()

	if err != nil {
		return
	}

	entry, err = d.unmarshal(line)

	if err != nil {
		err = fmt.Errorf("line %d: %w", d.line, err)
	}

	return
}
