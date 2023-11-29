echo "Downloading anime-offline-database-minified.json"
wget -P ./data/ https://raw.githubusercontent.com/manami-project/anime-offline-database/master/anime-offline-database-minified.json

echo "Downloading anidb-titles.dat"
wget --user-agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10.8; rv:21.0) Gecko/20100101 Firefox/21.0" \
     -O - https://anidb.net/api/anime-titles.dat.gz \
     | gunzip \
     > ./data/anidb-titles.dat

echo "Done!"