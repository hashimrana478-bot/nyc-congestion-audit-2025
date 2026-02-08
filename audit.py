import duckdb

class AuditEngine:
    """
    Forensic engine for detecting 'Ghost Trips' and Audit Leakage.
    Follows the Aggregation-First rule to maintain low memory footprints.
    """
    def __init__(self, con):
        self.con = con
        self.CZ_ZONE_IDS = [
            4, 12, 13, 43, 45, 48, 50, 68, 79, 87, 88, 90, 100, 103, 104, 105, 107, 113, 114, 
            125, 137, 140, 141, 142, 143, 144, 148, 158, 161, 162, 163, 164, 170, 186, 209, 
            211, 224, 229, 230, 231, 232, 233, 234, 236, 237, 238, 239, 246, 249, 261, 262, 263
        ]

    def create_forensic_audit(self):
        """
        Implements Ghost Trip Filters:
        1. Impossible Physics: Speed > 65 MPH (Average speed calculation).
        2. The Teleporter: Duration < 60s but Fare > $20.
        3. The Stationary Ride: Distance = 0 but Fare > 0.
        """
        print("Running Forensic Ghost Trip Audit...")
        
        self.con.execute(f"""
            CREATE OR REPLACE TABLE audit_results AS
            WITH calculations AS (
                SELECT 
                    *,
                    (epoch(dropoff_time) - epoch(pickup_time)) as duration_sec,
                    (trip_distance / (NULLIF(epoch(dropoff_time) - epoch(pickup_time), 0) / 3600.0)) as avg_speed_mph
                FROM trips_2025_raw
            )
            SELECT *,
                CASE 
                    WHEN avg_speed_mph > 65 THEN 'Impossible Physics'
                    WHEN duration_sec < 60 AND fare > 20 THEN 'Teleporter'
                    WHEN trip_distance = 0 AND fare > 0 THEN 'Stationary Ride'
                    ELSE NULL
                END as forensic_flag
            FROM calculations;
            
            -- Store suspicious records in a separate Audit Log
            CREATE OR REPLACE TABLE audit_log AS
            SELECT * FROM audit_results WHERE forensic_flag IS NOT NULL;
            
            -- Create clean table for downstream analytics
            CREATE OR REPLACE VIEW clean_trips AS
            SELECT * EXCLUDE (duration_sec, avg_speed_mph, forensic_flag) 
            FROM audit_results 
            WHERE forensic_flag IS NULL;
        """)
        
        stats = self.con.execute("SELECT forensic_flag, count(*) FROM audit_log GROUP BY 1").df()
        print("Audit Complete. Findings:")
        print(stats)

    def calculate_leakage(self):
        """Calculates surcharge compliance for trips entering the zone."""
        cz_list = ",".join(map(str, self.CZ_ZONE_IDS))
        
        query = f"""
            SELECT 
                pickup_loc,
                COUNT(*) as trip_count,
                AVG(CASE WHEN congestion_surcharge > 0 THEN 1 ELSE 0 END) as compliance_rate,
                SUM(CASE WHEN congestion_surcharge = 0 THEN 1 ELSE 0 END) as missing_surcharge_hits
            FROM clean_trips
            WHERE pickup_loc NOT IN ({cz_list}) AND dropoff_loc IN ({cz_list})
              AND pickup_time >= '2025-01-05'
            GROUP BY 1
            ORDER BY missing_surcharge_hits DESC
            LIMIT 3;
        """
        return self.con.execute(query).df()

    def compare_q1_volumes(self, path_2024_q1):
        """Phase 2.3: Compare Q1 2024 vs Q1 2025 trip volumes entering the zone."""
        cz_list = ",".join(map(str, self.CZ_ZONE_IDS))
        self.con.execute(f"CREATE OR REPLACE VIEW trips_24_q1 AS SELECT * FROM read_parquet('{path_2024_q1}')")
        
        query = f"""
            SELECT 
                '2024_Q1' as period,
                COUNT(*) as volume
            FROM trips_24_q1
            WHERE (PULocationID IN ({cz_list}) OR DOLocationID IN ({cz_list}))
            UNION ALL
            SELECT 
                '2025_Q1' as period,
                COUNT(*) as volume
            FROM clean_trips
            WHERE (pickup_loc IN ({cz_list}) OR dropoff_loc IN ({cz_list}))
              AND pickup_time BETWEEN '2025-01-01' AND '2025-03-31'
        """
        return self.con.execute(query).df()

    def get_suspicious_vendors(self):
        """Deliverable requirement: Top 5 Suspicious Vendors based on Ghost Trips."""
        return self.con.execute("""
            SELECT VendorID, forensic_flag, COUNT(*) as ghost_count
            FROM audit_log
            GROUP BY 1, 2
            ORDER BY ghost_count DESC
            LIMIT 5
        """).df()

if __name__ == "__main__":
    pass
