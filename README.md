# BirdSeed
Seeds quantities of csv stock data into a provided mysql database service.

## How to use
### Data
Add the selected stocks as csv to the /data directory. The program assumes that the data is in the format of: Date,Close/Last,Volume,Open,High,Low and that the first row is the header row.
See the 'ticker.csv' for an example.

### Database
The DSN connection string is read from the .env file located in the root dir. See the current file for format.