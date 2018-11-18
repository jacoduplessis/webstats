package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/ua-parser/uap-go/uaparser"
	"log"
	"net/url"
	"os"
	"strings"
	"time"
)

var nginxLogFormat = `
	"$host" "$http_cf_connecting_ip" "$http_cf_ipcountry" "$time_iso8601" "$status" "$request_method" "$request_uri" "$bytes_sent" "$request_time" "$http_referer" "$http_user_agent"
`

var uaCache = map[string]*uaparser.Client{}

func parse2(s string) []string {
	s = strings.Trim(s, "\"")
	parts := strings.Split(s, "\" \"")
	return parts
}

func setupDB(dbPath string) (*sql.DB, error) {

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(`
		PRAGMA JOURNAL_MODE=WAL;
		PRAGMA SYNCHRONOUS=NORMAL;

		CREATE TABLE IF NOT EXISTS entries (
			host TEXT,
			remote_addr TEXT,
			country_code TEXT,
			time TEXT,
			method TEXT,
			path TEXT,
			status_code INT,
			size INT,
		  	response_time REAL,
			referrer_url TEXT,
			referrer_domain TEXT,
			ua_string TEXT,
			ua_client_family TEXT,
			ua_client_version INT,
			ua_os_family TEXT,
			ua_os_version INT,
			ua_device_family TEXT,
			ua_device_brand TEXT,
			ua_device_model TEXT
		)
	`)

	return db, err

}

func main() {

	var dbPath string
	var logPath string
	var trunc bool

	flag.StringVar(&dbPath, "db", "webstats.db", "path to db file")
	flag.StringVar(&logPath, "log", "access.log", "path to log file")
	flag.BoolVar(&trunc, "trunc", false, "truncate the log file")

	flag.Parse()

	db, err := setupDB(dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()


	f, _ := os.OpenFile(logPath, os.O_RDWR, 0666)
	defer f.Close()
	scanner := bufio.NewScanner(f)
	parser := uaparser.NewFromSaved()

	nCols := 19

	valueString := strings.Repeat("?,", nCols)
	valueString = strings.TrimRight(valueString, ",")
	valueString = fmt.Sprintf("(%s)", valueString)

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := tx.Prepare(fmt.Sprintf("INSERT INTO entries VALUES %s", valueString))
	if err != nil {
		log.Fatal(err)
	}

	var numInserted int

	startTime := time.Now()

	for scanner.Scan() {

		parts := parse2(scanner.Text())

		host := parts[0]
		remoteAddr := parts[1]
		countryCode := parts[2]
		timeISO8601 := parts[3]
		statusCode := parts[4]
		requestMethod := parts[5]
		requestPath := parts[6]
		requestSize := parts[7]
		responseTime := parts[8]
		rawReferrer := parts[9]
		uaString := parts[10]

		requestPath, _ = url.QueryUnescape(requestPath)

		ua, ok := uaCache[uaString]
		if !ok {
			ua = parser.Parse(uaString)
			uaCache[uaString] = ua
		}

		ref, _ := url.Parse(rawReferrer)
		var refURL string
		if rawReferrer != "-" {
			refURL = rawReferrer
		}

		var a []interface{}


		a = append(a,
			host,
			remoteAddr,
			countryCode,
			timeISO8601,
			requestMethod,
			requestPath,
			statusCode,
			requestSize,
			responseTime,
			refURL,
			ref.Host,
			uaString,
			ua.UserAgent.Family,
			ua.UserAgent.Major,
			ua.Os.Family,
			ua.Os.Major,
			ua.Device.Family,
			ua.Device.Brand,
			ua.Device.Model,
		)

		_, err = stmt.Exec(a...)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error inserting row: %v\n", err)
		} else {
			numInserted++
		}

	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error parsing file: %#v\n", err)
	}

	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}

	elapsed := time.Since(startTime)
	fmt.Printf("Time: %.6f\n", elapsed.Seconds())

	fmt.Fprintf(os.Stderr, "Rows inserted: %d\nTruncated: %v\n", numInserted, trunc)

	if trunc {
		f.Truncate(0)
		f.Seek(0, 0)
		f.Sync()
	}

}
