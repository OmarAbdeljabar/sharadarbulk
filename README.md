# sharadarbulkload
Downloads Sharadar data and uploads to PostgreSQL.

## Setup
1. Clone: `git clone https://github.com/OmarAbdeljabar/sharadarbulkload.git`
2. Install: `pip install -r requirements.txt`
3. Set env vars: `NASDAQKEY`
4. Run BulkCsvDownload.py
5. Configure postgreSQL database paramaters:  
    "dbname": "sharadar2",
    "user": "postgres",
    "password": "123456",
    "host": "localhost",
    "port": "5432"
6. Run Postgresinsert.py