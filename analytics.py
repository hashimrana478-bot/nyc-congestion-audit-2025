import duckdb
import requests
import pandas as pd
import numpy as np

class AnalyticsEngine:
    """
    Analytics engine for geospatial and weather-driven insights.
    Integrates live weather API data with DuckDB trip counts.
    """
    def __init__(self, con, cz_ids):
        self.con = con
        self.cz_ids = cz_ids

    def get_border_effect(self, path_2024):
        """
        Calculates shift in drop-offs for zones immediately bordering the 60th St line.
        """
        print("Calculating Border Effect (Latitudinal Shift)...")
        
        # Border Zones (TLC IDs for zones near Central Park South / Midtown North)
        border_zone_ids = [236, 237, 238, 239, 140, 141, 142, 143, 262, 263]
        bz_list = ",".join(map(str, border_zone_ids))
        
        self.con.execute(f"CREATE OR REPLACE VIEW trips_2024_all AS SELECT * FROM read_parquet('{path_2024}')")
        
        query = f"""
            WITH base_24 AS (
                SELECT DOLocationID as loc_id, COUNT(*) as count_24
                FROM trips_2024_all
                WHERE DOLocationID IN ({bz_list})
                GROUP BY 1
            ),
            base_25 AS (
                SELECT dropoff_loc as loc_id, COUNT(*) as count_25
                FROM clean_trips
                GROUP BY 1
            )
            SELECT 
                b24.loc_id, b24.count_24, b25.count_25,
                (CAST(b25.count_25 AS FLOAT) - b24.count_24) / b24.count_24 as change_rate
            FROM base_24 b24
            JOIN base_25 b25 ON b24.loc_id = b25.loc_id;
        """
        return self.con.execute(query).df()

    def get_velocity_comparison(self, path_2024_q1):
        """Generates heatmap data for speed before/after toll."""
        cz_list = ",".join(map(str, self.cz_ids))
        
        self.con.execute(f"CREATE OR REPLACE VIEW trips_2024_q1 AS SELECT * FROM read_parquet('{path_2024_q1}')")
        
        q_v24 = f"""
            SELECT 
                hour(tpep_pickup_datetime) as hour,
                dayname(tpep_pickup_datetime) as dow,
                avg(trip_distance / (NULLIF(epoch(tpep_dropoff_datetime) - epoch(tpep_pickup_datetime), 0) / 3600.0)) as speed
            FROM trips_2024_q1
            WHERE PULocationID IN ({cz_list}) AND DOLocationID IN ({cz_list})
            GROUP BY 1, 2
        """
        
        q_v25 = f"""
            SELECT 
                hour(pickup_time) as hour,
                dayname(pickup_time) as dow,
                avg(trip_distance / (NULLIF(epoch(dropoff_time) - epoch(pickup_time), 0) / 3600.0)) as speed
            FROM clean_trips
            WHERE pickup_loc IN ({cz_list}) AND dropoff_loc IN ({cz_list})
              AND pickup_time BETWEEN '2025-01-01' AND '2025-03-31'
            GROUP BY 1, 2
        """
        
        df24 = self.con.execute(q_v24).df()
        df25 = self.con.execute(q_v25).df()
        return df24, df25

    def get_tip_crowding_analysis(self):
        """Phase 3.3: Tip 'Crowding Out' Analysis.
        Hypothesis: Higher tolls reduce disposable income for tips.
        Returns monthly avg surcharge and tip percentage.
        """
        print("Calculating Tip 'Crowding Out' Analysis...")
        
        query = """
            SELECT 
                EXTRACT(MONTH FROM pickup_time) as month,
                AVG(congestion_surcharge) as avg_surcharge,
                AVG(
                    CASE 
                        WHEN fare > 0 THEN ((total_amount - fare - congestion_surcharge) / fare) * 100
                        ELSE 0 
                    END
                ) as avg_tip_pct
            FROM clean_trips
            WHERE pickup_time >= '2025-01-05'
            GROUP BY 1
            ORDER BY 1
        """
        return self.con.execute(query).df()

    def get_rain_elasticity(self, year=2025):
        """Fetches Open-Meteo weather data and joins with trip volume."""
        print("Evaluating Rain Elasticity...")
        
        # NOAA-style Central Park Weather (NY)
        url = f"https://archive-api.open-meteo.com/v1/archive?latitude=40.78&longitude=-73.96&start_date={year}-01-01&end_date={year}-12-31&daily=precipitation_sum&timezone=America%2FNew_York"
        
        try:
            weather = requests.get(url).json()
            weather_df = pd.DataFrame({
                "date": pd.to_datetime(weather["daily"]["time"]),
                "precip_mm": weather["daily"]["precipitation_sum"]
            })
        except:
            return None, 0

        trips = self.con.execute("""
            SELECT CAST(pickup_time AS DATE) as date, count(*) as daily_trips
            FROM clean_trips
            GROUP BY 1
        """).df()
        trips['date'] = pd.to_datetime(trips['date'])
        
        merged = pd.merge(trips, weather_df, on="date")
        correlation = merged['daily_trips'].corr(merged['precip_mm'])
        
        return merged, correlation

if __name__ == "__main__":
    pass
