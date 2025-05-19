import os
import psycopg2
import csv
import time
import pandas as pd

# ─── Configuration ───────────────────────────────────────────────────────────────
db_params = {
    "dbname": "sharadar2",
    "user": "postgres",
    "password": "123456",
    "host": "localhost",
    "port": "5432"
}

data_folder = r"C:\Sharadar Data"
csv_files = [
    "SF1.csv", "SF2.csv", "SF3.csv", "EVENTS.csv", "SF3A.csv", "SF3B.csv",
    "SEP.csv", "TICKERS.csv", "INDICATORS.csv", "DAILY.csv", "SP500.csv",
    "ACTIONS.csv", "SFP.csv", "METRICS.csv"
]

log_file = os.path.join(data_folder, "upload_log.txt")

# Load INDICATORS.csv to map column types
indicators_path = os.path.join(data_folder, "INDICATORS.csv")
indicators_df = pd.read_csv(indicators_path)

# Map unittype to PostgreSQL data types
type_mapping = {
    "currency": "NUMERIC",
    "currency/share": "NUMERIC",
    "USD": "NUMERIC",
    "USD/share": "NUMERIC",
    "USD millions": "NUMERIC",
    "units": "NUMERIC",
    "numeric": "NUMERIC",
    "ratio": "NUMERIC",
    "percent": "NUMERIC",
    "%": "NUMERIC",
    "date (YYYY-MM-DD)": "DATE",
    "text": "TEXT",
    "Y/N": "BOOLEAN",
    "N/A": "TEXT"
}

# ─── Helper Functions ───────────────────────────────────────────────────────────
def log(message):
    with open(log_file, "a", encoding="utf-8") as logf:
        logf.write(message + "\n")
    print(message)

def get_column_type(table, column):
    if table.lower() == "sf1" and column.lower() == "fiscalperiod":
        return "TEXT"
    match = indicators_df[
        (indicators_df["table"].str.lower() == table.lower()) &
        (indicators_df["indicator"].str.lower() == column.lower())
    ]
    if not match.empty:
        unittype = match.iloc[0]["unittype"]
        return type_mapping.get(unittype, "TEXT")
    return "TEXT"

def preprocess_indicators_csv(file_path):
    """Preprocess INDICATORS.csv to quote only description fields with commas."""
    temp_file = file_path + ".tmp"
    with open(file_path, 'r', encoding='utf-8', errors='replace') as f_in, \
         open(temp_file, 'w', encoding='utf-8', newline='') as f_out:
        reader = csv.reader(f_in)
        writer = csv.writer(f_out, quoting=csv.QUOTE_NONE, escapechar='\\')
        headers = next(reader)
        writer.writerow(headers)  # Write headers as-is
        for row in reader:
            new_row = []
            for i, col in enumerate(row):
                # Quote only description column (index 5) if it contains commas
                if i == 5 and ',' in col:
                    new_row.append(f'"{col}"')
                else:
                    new_row.append(col)
            writer.writerow(new_row)
    return temp_file

def upload_csv(file_path, conn):
    table_name = os.path.splitext(os.path.basename(file_path))[0].lower()
    start = time.time()

    try:
        # Preprocess INDICATORS.csv if needed
        if table_name == "indicators":
            file_path = preprocess_indicators_csv(file_path)
        else:
            file_path = file_path

        # Read headers from CSV
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.reader(f)
            headers = next(reader)

        # Determine column types
        column_types = {col: get_column_type(table_name, col) for col in headers}

        # Generate CREATE TABLE statement
        column_defs = ', '.join(f'"{col}" {column_types[col]}' for col in headers)

        # Get composite primary key columns
        pk_list = indicators_df[
            (indicators_df["table"].str.lower() == table_name) &
            (indicators_df["isprimarykey"] == "Y")
        ]["indicator"].str.lower().tolist()

        with conn.cursor() as cur:
            # Drop and recreate table without primary key
            cur.execute(f'DROP TABLE IF EXISTS "{table_name}";')
            cur.execute(f'CREATE TABLE "{table_name}" ({column_defs});')

            # Stream data to PostgreSQL
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                next(f)  # Skip header
                cur.copy_from(f, table_name, sep=',', columns=headers, null='')

            # Add primary key constraint after data load
            if pk_list:
                pk_columns = ', '.join(f'"{pk}"' for pk in pk_list)
                cur.execute(f'ALTER TABLE "{table_name}" ADD PRIMARY KEY ({pk_columns});')

            # Get row count for logging
            cur.execute(f'SELECT COUNT(*) FROM "{table_name}";')
            row_count = cur.fetchone()[0]

        conn.commit()
        elapsed = time.time() - start
        log(f"✓ {table_name}: {row_count} rows in {elapsed:.2f}s")

    except Exception as e:
        conn.rollback()
        log(f"❌ {table_name}: ERROR – {str(e)}")

    finally:
        # Clean up temporary file for indicators
        if table_name == "indicators" and os.path.exists(file_path):
            os.remove(file_path)

# ─── Main Execution ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Remove existing log if present
    if os.path.exists(log_file):
        os.remove(log_file)

    # Connect and upload each file
    with psycopg2.connect(**db_params) as conn:
        for file in csv_files:
            full_path = os.path.join(data_folder, file)
            if os.path.exists(full_path):
                upload_csv(full_path, conn)
            else:
                log(f"⚠️ Missing file: {file}")