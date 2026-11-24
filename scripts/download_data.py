import os
import urllib.request

BASE_URL = "https://d37ci6vzurychx.cloudfront.net"   # official NYC TLC file server
RAW_DIR = "data/raw"                                  # raw data lives here (never modified)
TRIPS_DIR = os.path.join(RAW_DIR, "yellow")           # trip files in their own subfolder
YEARS = [2022, 2023, 2024]

def download(url, dest):
    # Skip files we already have -> script is safe to re-run (idempotent)
    if os.path.exists(dest):
        print(f"skip: {os.path.basename(dest)}")
        return
    print(f"downloading: {os.path.basename(dest)}")
    # Some servers block default Python user agent, so we set a browser-like one
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as response, open(dest, "wb") as f:
        f.write(response.read())

os.makedirs(TRIPS_DIR, exist_ok=True)   # exist_ok -> no error if folder exists

# Small dimension table (used later for broadcast join)
download(f"{BASE_URL}/misc/taxi_zone_lookup.csv",
         os.path.join(RAW_DIR, "taxi_zone_lookup.csv"))

# Monthly fact files: yellow_tripdata_2022-01.parquet ... 2024-12
for year in YEARS:
    for month in range(1, 13):
        name = f"yellow_tripdata_{year}-{month:02d}.parquet"   # :02d -> 1 becomes "01"
        download(f"{BASE_URL}/trip-data/{name}", os.path.join(TRIPS_DIR, name))

total_bytes = sum(os.path.getsize(os.path.join(TRIPS_DIR, f)) for f in os.listdir(TRIPS_DIR))
print(f"Total trip data: {total_bytes / 1024**3:.2f} GB")