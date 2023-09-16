package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
	_ "github.com/libsql/libsql-client-go/libsql"
)

const (
	layoutISO = "2006-01-02"
	layoutUS  = "01/02/2006"
	DSN       = "DSN"
)

// Candle is a single candle of a time series
type Candle struct {
	ID     int64
	Ticker string

	Date   time.Time
	Open   float64
	Close  float64
	High   float64
	Low    float64
	Volume int64
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

func run() error {
	// Load environment variables to get database DSN
	err := loadEnvironmentVariables()
	if err != nil {
		return fmt.Errorf("could not load .env file at '../.env'. %w", err)
	}

	db, err := connectToDatabase()
	if err != nil {
		return err
	}

	// Loads .csv files from ../data/ using ticker.csv naming convention
	// Assumes that the data is in the format of: Date,Close/Last,Volume,Open,High,Low
	// and that the first row is the header row
	candles, err := aggregateCandlesFromFiles(db)
	if err != nil {
		return fmt.Errorf("could not load data from csv files. %w", err)
	}

	// Seed the data into the database
	err = seed(db, candles)
	if err != nil {
		return fmt.Errorf("could not seed data. %w", err)
	}

	return nil
}

func loadEnvironmentVariables() error {
	return godotenv.Load("../.env")
}

func connectToDatabase() (*sqlx.DB, error) {
	url := os.Getenv("DSN")

	db, err := sqlx.Open("libsql", url)
	if err != nil {
		return nil, err
	}
	log.Print("Successfully opened connection to database.")

	if db.Ping(); err != nil {
		return nil, fmt.Errorf("could not ping database. %w", err)
	}
	log.Print("Successfully pinged database.")

	return db, err
}

func aggregateCandlesFromFiles(db *sqlx.DB) ([]Candle, error) {
	// read each file and create all candles to be seeded
	files, err := os.ReadDir("../data/")
	if err != nil {
		return nil, err
	}

	candles := []Candle{}
	for _, f := range files {
		// If data with ticker exists, abort.
		ticker := strings.Split(f.Name(), ".")[0]
		var count int64
		err := db.Get(&count, "SELECT COUNT(1) FROM candles WHERE ticker = ?", ticker)
		if err != nil {
			log.Printf("ERR: %s", err.Error())
		}
		log.Printf("COUNT: %d", count)
		if count > 0 {
			log.Printf("Data for ticker '%s' already exists. Skipping.", ticker)
			continue
		}

		log.Printf("Inserting data for '%s'.", ticker)

		c, err := createCandles(f.Name())
		if err != nil {
			return nil, err
		}

		candles = append(candles, c...)
	}

	return candles, nil
}

func seed(db *sqlx.DB, c []Candle) error {
	if len(c) == 0 {
		log.Print("No data to seed.")
		return nil
	}

	err := bulkInsert(db, c)

	if err == nil {
		log.Print("Successfully inserted data.")
	}

	return err
}

func createCandles(s string) ([]Candle, error) {
	ticker := strings.Split(s, ".")[0]

	// Open the file
	f, err := os.Open("../data/" + s)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Read the file into a string matrix
	reader := csv.NewReader(f)
	data, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	// Convert the data into candles, skipping the csv header row
	candles := make([]Candle, 0, len(data))
	for _, d := range data[1:] {
		candle, err := createCandle(ticker, d)
		if err != nil {
			return nil, err
		}

		candles = append(candles, candle)
	}

	return candles, nil
}

func createCandle(ticker string, s []string) (Candle, error) {
	date, err := time.Parse(layoutISO, s[0])
	if err != nil {
		return Candle{}, err
	}

	open, err := parse(clean(s[1]))
	if err != nil {
		log.Fatal(err)
	}

	high, err := parse(clean(s[2]))
	if err != nil {
		log.Fatal(err)
	}

	low, err := parse(clean(s[3]))
	if err != nil {
		log.Fatal(err)
	}

	close, err := parse(clean(s[4]))
	if err != nil {
		log.Fatal(err)
	}

	volume, err := strconv.ParseInt(s[6], 10, 64)
	if err != nil {
		volume = 0
	}

	candle := Candle{
		Ticker: ticker,
		Date:   date,
		Open:   open,
		Close:  close,
		High:   high,
		Low:    low,
		Volume: volume,
	}

	return candle, nil
}

func clean(s string) string {
	return strings.Replace(s, "$", "", -1)
}

func parse(s string) (float64, error) {
	value, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}

	return value, nil
}

func bulkInsert(db *sqlx.DB, candles []Candle) error {
	BUF_LENGTH := 50
	PARAM_LENGTH := 7
	INSERTS_PER_TX := 10

	var values []interface{}
	for _, c := range candles {
		values = append(values, c.Date.Format("2006-01-02"), c.Ticker, c.Open, c.High, c.Low, c.Close, c.Volume)

		// Number of values to insert per candle.
		if len(values) == BUF_LENGTH*PARAM_LENGTH*INSERTS_PER_TX {
			err := insertNPerTx(db, values, BUF_LENGTH, PARAM_LENGTH, INSERTS_PER_TX)
			if err != nil {
				return err
			}
			values = values[0:0]
		}
	}

	if len(values) > 0 {
		err := insertNPerTx(db, values, len(values)/PARAM_LENGTH, PARAM_LENGTH, 1)
		if err != nil {
			return err
		}
	}

	return nil
}

func insertNPerTx(db *sqlx.DB, values []interface{}, buf_len int, param_len int, n int) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	bufLengthStmt := insertNCandlesStatement(buf_len)

	stmt, err := tx.Prepare(bufLengthStmt)
	if err != nil {
		return err
	}

	for i := 0; i < n; i++ {
		if _, err := stmt.Exec(values[i*buf_len*param_len : (i+1)*buf_len*param_len]...); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func insertNCandlesStatement(n int) string {
	buf := bytes.NewBuffer([]byte("INSERT INTO candles (date, ticker, open, high, low, close, volume) VALUES "))
	for i := 0; i < n; i++ {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString("(?,?,?,?,?,?,?)")
	}

	return buf.String()
}
