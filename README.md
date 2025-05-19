# sharadarbulk
Downloads Sharadar data and uploads to PostgreSQL.

## Setup
1. Clone: `git clone https://github.com/OmarAbdeljabar/sharadarbulkload.git`
2. Install: `pip install -r requirements.txt`
3. Set env vars: `NASDAQKEY`
4. Set 'DEST_DIR' in 'BulkCsvDownload.py' 
5. Run: 'BulkCsvDownload.py' 
6. Configure postgreSQL database paramaters in 'Postgresinsert.py':
    "dbname": "sharadar2",
    "user": "postgres",
    "password": "123456",
    "host": "localhost",
    "port": "5432"
6. Set 'DEST_DIR' in 'Postgresinsert.py'
7. Run: 'Postgresinsert.py'
