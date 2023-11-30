package otame

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"fmt"

	"github.com/mattn/go-sqlite3"
)

const driverName = "sqlite3_with_rank_udf"

type MatchInfo struct {
	NumPhrase  int32
	NumColumn  int32
	PhraseInfo []int32
}

func (mi *MatchInfo) GetPhraseInfo(phraseNumber, colNumber int) (hitCount int, globalHitCount int) {
	phraseInfoIndex := (phraseNumber * int(mi.NumColumn) * 3)

	if len(mi.PhraseInfo) < phraseInfoIndex+colNumber*3+1 {
		panic(
			fmt.Sprintf(
				"invalid phrase info index: %d (iPhrase=%d, iColumn=%d)",
				phraseInfoIndex+colNumber*3+1,
				phraseNumber,
				colNumber,
			),
		)
	}

	hitCount = int(mi.PhraseInfo[phraseInfoIndex+colNumber*3])
	globalHitCount = int(mi.PhraseInfo[phraseInfoIndex+colNumber*3+1])
	return
}

func (mi *MatchInfo) Read(p []byte) (err error) {
	buff := bytes.NewBuffer(p)

	if err = binary.Read(buff, binary.LittleEndian, &mi.NumPhrase); err != nil {
		return
	}

	if err = binary.Read(buff, binary.LittleEndian, &mi.NumColumn); err != nil {
		return
	}

	mi.PhraseInfo = make([]int32, 3*mi.NumPhrase*mi.NumColumn)

	if err = binary.Read(buff, binary.LittleEndian, &mi.PhraseInfo); err != nil {
		return
	}

	return
}

type RankFunc func(*MatchInfo) float64

var DefaultRankFunc RankFunc = func(mi *MatchInfo) float64 {
	var rank float64

	for i := 0; i < int(mi.NumPhrase); i++ {
		for j := 0; j < int(mi.NumColumn); j++ {
			hitCount, globalHitCount := mi.GetPhraseInfo(i, j)

			if hitCount > 0 {
				rank += float64(hitCount) / float64(globalHitCount)
			}
		}
	}

	return rank
}

func init() {
	sql.Register(driverName, &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) (err error) {
			err = conn.RegisterFunc("rank", func(miBytes []byte) (rank float64) {
				var mi MatchInfo
				err = mi.Read(miBytes)

				if err != nil {
					panic(fmt.Sprint("failed to read matchinfo: ", err))
				}

				rank = DefaultRankFunc(&mi)

				return
			}, true)

			return
		},
	})
}
