package main

import (
	"encoding/csv"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const (
	layoutISO = "2006-01-02"
	layoutUS  = "01/02/2006"
)

// Candle is a single candle of a time series
type Candle struct {
	ID     string    `gorm:"primaryKey"`
	Ticker string    `gorm:"not null; index"`
	Date   time.Time `gorm:"not null"`
	Open   float64   `gorm:"not null"`
	Close  float64   `gorm:"not null"`
	High   float64   `gorm:"not null"`
	Low    float64   `gorm:"not null"`
	Volume int64     `gorm:"not null"`
}

func main() {
	// Load environment variables to get database DSN
	err := loadEnvironmentVariables()
	if err != nil {
		log.Fatal("Could not load .env file at '../.env'")
		os.Exit(1)
	}

	db, err := connectToDatabase()
	if err != nil {
		panic(err)
	}

	err = db.AutoMigrate(&Candle{})
	if err != nil {
		log.Fatal("Could not migrate database.")
		panic(err)
	}

	// Loads .csv files from ../data/ using ticker.csv naming convention
	// Assumes that the data is in the format of: Date,Close/Last,Volume,Open,High,Low
	// and that the first row is the header row
	candles, err := aggregateCandlesFromFiles(db)
	if err != nil {
		log.Fatal("Could not load data from csv files.")
		panic(err)
	}

	// Seed the data into the database
	err = seed(db, candles)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}

func loadEnvironmentVariables() error {
	return godotenv.Load("../.env")
}

func connectToDatabase() (*gorm.DB, error) {
	dsn := os.Getenv("DSN")
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	log.Print("Successfully opened connection to database.")

	sqlDB, err := db.DB()
	if sqlDB.Ping(); err != nil {
		return nil, err
	}
	log.Print("Successfully pinged database.")

	return db, err
}

func aggregateCandlesFromFiles(db *gorm.DB) ([]Candle, error) {
	// read each file and create all candles to be seeded
	files, err := ioutil.ReadDir("../data/")
	if err != nil {
		return nil, err
	}

	candles := []Candle{}
	for _, f := range files {
		// If data with ticker exists, abort.
		ticker := strings.Split(f.Name(), ".")[0]
		var count int64
		db.Model(&Candle{}).Where("ticker = ?", ticker).Count(&count)
		if count > 0 {
			log.Print("Data for ticker '" + ticker + "' already exists. Skipping.")
			continue
		}

		c, err := createCandles(f.Name())
		if err != nil {
			return nil, err
		}

		candles = append(candles, c...)
	}

	return candles, nil
}

func seed(db *gorm.DB, c []Candle) error {
	if len(c) == 0 {
		log.Print("No data to seed.")
		return nil
	}

	log.Print("Inserting data for '" + c[0].Ticker + "'.")

	err := db.Transaction(func(tx *gorm.DB) error {
		tx.CreateInBatches(c, 100)

		return nil
	})

	if err == nil {
		log.Print("Successfully inserted data for '" + c[0].Ticker + "'.")
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
	date, err := time.Parse(layoutUS, s[0])
	if err != nil {
		return Candle{}, err
	}

	close, err := parse(clean(s[1]))
	if err != nil {
		log.Fatal(err)
	}

	volume, err := strconv.ParseInt(s[2], 10, 64)
	if err != nil {
		volume = 0
	}

	open, err := parse(clean(s[3]))
	if err != nil {
		log.Fatal(err)
	}

	high, err := parse(clean(s[4]))
	if err != nil {
		log.Fatal(err)
	}

	low, err := parse(clean(s[5]))
	if err != nil {
		log.Fatal(err)
	}

	candle := Candle{
		ID:     uuid.NewString(),
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
