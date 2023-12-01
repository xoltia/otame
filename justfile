buildtags := "icu fts4"

build PKG:
    CGO_ENABLED=1
    go build -tags "{{buildtags}}" -o ./bin/{{PKG}} ./cmd/{{PKG}}

_check-data:
    if [ ! -d "./data" ]; then \
        just download-sources; \
        exit 1; \
    fi

gen-db DB="otame.sqlite3": _check-data
    go run -tags "{{buildtags}}" ./cmd/gen -o {{DB}}

update-db DB="otame.sqlite3":
    go run -tags "{{buildtags}}" ./cmd/update -o {{DB}}

download-sources:
    @echo "Downloading anime-offline-database-minified.json"
    wget -P ./data/ https://raw.githubusercontent.com/manami-project/anime-offline-database/master/anime-offline-database-minified.json

    @echo "Downloading anidb-titles.dat"
    wget --user-agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10.8; rv:21.0) Gecko/20100101 Firefox/21.0" \
        -O - https://anidb.net/api/anime-titles.dat.gz \
        | gunzip > ./data/anidb-titles.dat

    @echo "Downloading vndb-db-latest.tar.zst"
    wget -P ./data/ https://dl.vndb.org/dump/vndb-db-latest.tar.zst
    mkdir -p ./data/vndb-db-latest
    tar -I zstd -xf ./data/vndb-db-latest.tar.zst -C ./data/vndb-db-latest

    @echo "Done!"
