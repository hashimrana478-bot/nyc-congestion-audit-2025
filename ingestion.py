import os
import requests
import duckdb
import pandas as pd
from datetime import datetime

class DataIngestor:
    """
    High-fidelity data ingestion layer for NYC TLC data.
    Implements strict weighted imputation for missing 2025 data.
    """
    def __init__(self, data_dir="data"):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)
        self.con = duckdb.connect(database=':memory:')
        self.con.execute("SET memory_limit = '1GB';")

    def download_file(self, taxi_type, year, month):
        file_name = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
        target_path = os.path.join(self.data_dir, file_name)

        if os.path.exists(target_path):
            print(f"✓ {file_name} already exists (skipping download)")
            return target_path

        print(f"⬇️  Downloading {file_name}...")
        try:
            response = requests.get(url, stream=True, timeout=60)
            if response.status_code == 200:
                total_size = int(response.headers.get('content-length', 0))
                downloaded = 0
                with open(target_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=65536):
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_size > 0:
                            pct = (downloaded / total_size) * 100
                            print(f"  Progress: {pct:.1f}%", end='\r')
                print(f"\n✓ Downloaded {file_name} successfully")
                return target_path
            elif response.status_code == 404:
                print(f"⚠️  {file_name} not found on server (404)")
                return None
            else:
                print(f"❌ Failed: HTTP {response.status_code}")
                return None
        except requests.exceptions.Timeout:
            print(f"❌ Timeout downloading {file_name}. Retrying once...")
            try:
                response = requests.get(url, stream=True, timeout=120)
                if response.status_code == 200:
                    with open(target_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=65536):
                            f.write(chunk)
                    print(f"✓ Downloaded {file_name} successfully (retry)")
                    return target_path
            except:
                print(f"❌ Retry failed for {file_name}")
                return None
        except Exception as e:
            print(f"❌ Error downloading {file_name}: {str(e)[:100]}")
            return None

    def impute_december_2025(self, taxi_type):
        """
        Implements the 0.3/0.7 Weighted Imputation Rule.
        Value_2025 = (Value_2023 * 0.3) + (Value_2024 * 0.7)
        """
        path_23 = self.download_file(taxi_type, 2023, 12)
        path_24 = self.download_file(taxi_type, 2024, 12)
        target_path = os.path.join(self.data_dir, f"{taxi_type}_tripdata_2025-12.parquet")

        if not path_23 or not path_24:
            raise FileNotFoundError(f"Historical data for {taxi_type} imputation missing.")

        print(f"Applying Weighted Imputation for {taxi_type} Dec 2025...")
        
        # We use DuckDB's streaming engine to sample and cast schemas
        # Schema mapping is crucial because column aliases change over years.
        
        ts_col = "tpep_pickup_datetime" if taxi_type == 'yellow' else "lpep_pickup_datetime"
        ds_col = "tpep_dropoff_datetime" if taxi_type == 'yellow' else "lpep_dropoff_datetime"

        query = f"""
        COPY (
            SELECT 
                CAST({ts_col} + INTERVAL '2 YEAR' AS TIMESTAMP) as {ts_col},
                CAST({ds_col} + INTERVAL '2 YEAR' AS TIMESTAMP) as {ds_col},
                * EXCLUDE ({ts_col}, {ds_col})
            FROM read_parquet('{path_23}') USING SAMPLE 30%
            UNION ALL
            SELECT 
                CAST({ts_col} + INTERVAL '1 YEAR' AS TIMESTAMP) as {ts_col},
                CAST({ds_col} + INTERVAL '1 YEAR' AS TIMESTAMP) as {ds_col},
                * EXCLUDE ({ts_col}, {ds_col})
            FROM read_parquet('{path_24}') USING SAMPLE 70%
        ) TO '{target_path}' (FORMAT PARQUET);
        """
        self.con.execute(query)
        return target_path

    def run_full_2025_ingestion(self):
        """Downloads all 2025 files and triggers imputation where data is missing."""
        for taxi in ['yellow', 'green']:
            for month in range(1, 13):
                path = self.download_file(taxi, 2025, month)
                if not path and month == 12:
                    self.impute_december_2025(taxi)

    def create_unified_view(self):
        """Unifies Yellow and Green schemas into a production-ready view."""
        y_glob = os.path.join(self.data_dir, "yellow_tripdata_2025-*.parquet")
        g_glob = os.path.join(self.data_dir, "green_tripdata_2025-*.parquet")

        self.con.execute(f"""
            CREATE OR REPLACE VIEW trips_2025_raw AS
            SELECT 
                tpep_pickup_datetime as pickup_time, tpep_dropoff_datetime as dropoff_time,
                PULocationID as pickup_loc, DOLocationID as dropoff_loc,
                trip_distance, fare_amount as fare, total_amount,
                COALESCE(congestion_surcharge, 0) as congestion_surcharge,
                'yellow' as taxi_type, VendorID
            FROM read_parquet('{y_glob}')
            UNION ALL
            SELECT 
                lpep_pickup_datetime as pickup_time, lpep_dropoff_datetime as dropoff_time,
                PULocationID as pickup_loc, DOLocationID as dropoff_loc,
                trip_distance, fare_amount as fare, total_amount,
                COALESCE(congestion_surcharge, 0) as congestion_surcharge,
                'green' as taxi_type, VendorID
            FROM read_parquet('{g_glob}')
        """)
        print("Schema Unification Complete.")

if __name__ == "__main__":
    ingestor = DataIngestor()
    ingestor.run_full_2025_ingestion()
    ingestor.create_unified_view()
