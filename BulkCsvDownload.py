import os, json, time, zipfile, io, pathlib, requests
from tqdm import tqdm

API_KEY   = os.getenv("NASDAQKEY")  # ← automatically picked up
DEST_DIR  = pathlib.Path(r"C:\Sharadar Data")
DEST_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL  = "https://data.nasdaq.com/api/v3/datatables/SHARADAR/{table}.json"
TABLES    = [
    "SF1","SF2","SF3","EVENTS","SF3A","SF3B",
    "SEP","TICKERS","INDICATORS","DAILY",
    "SP500","ACTIONS","SFP","METRICS",
]

def get_bulk_download_link(table: str) -> str:
    """
    Ask Nasdaq Data Link for a bulk‑download link for the given table
    (uses ?qopts.export=true).  Keeps polling until the file is ready.
    """
    url   = f"{BASE_URL.format(table=table)}?qopts.export=true&api_key={API_KEY}"
    valid = {"fresh", "regenerating"}  # statuses that include a ready link
    print(f"\nPreparing bulk file for {table} …")
    while True:
        resp   = requests.get(url, timeout=60)
        resp.raise_for_status()
        meta   = resp.json()["datatable_bulk_download"]["file"]
        status = meta["status"]
        if status in valid:
            return meta["link"]
        # status == "generating" ➜ wait a bit and poll again
        time.sleep(60)

def download_and_extract(link: str, dest: pathlib.Path):
    """
    Stream‑download the ZIP at `link` and extract the enclosed CSV to `dest`.
    """
    with requests.get(link, stream=True, timeout=120) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length", "0"))
        buf   = io.BytesIO()
        for chunk in tqdm(r.iter_content(chunk_size=1 << 20), 
                          total=total // (1 << 20) + 1,
                          unit="MB", unit_scale=True,
                          desc="Downloading"):
            buf.write(chunk)
        buf.seek(0)
    with zipfile.ZipFile(buf) as zf:
        member = zf.namelist()[0]          # only one CSV inside
        zf.extract(member, dest.parent)
        (dest.parent / member).replace(dest)  # rename to clean name

for table in TABLES:
    csv_path = DEST_DIR / f"{table}.csv"
    if csv_path.exists():
        print(f"✓ {csv_path.name} already exists – skipping.")
        continue
    link = get_bulk_download_link(table)
    print(f"Link ready ➜ downloading {table}.csv …")
    download_and_extract(link, csv_path)
    print(f"✓ Saved → {csv_path}")

print("\nAll requested tables downloaded successfully!")
