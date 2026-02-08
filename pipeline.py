import os
import duckdb
import pandas as pd
from ingestion import DataIngestor
from audit import AuditEngine
from analytics import AnalyticsEngine

def run_production_pipeline():
    """
    Main orchestration entry point.
    Strictly follows the 'Aggregation-First' and 'Memory-Limit' rules.
    """
    print("="*60)
    print("🚀 NYC CONGESTION PRICING AUDIT - PRODUCTION PIPELINE")
    print("="*60)
    print("\n⚙️  System Configuration:")
    print("   - DuckDB Memory Limit: 1GB")
    print("   - Processing Mode: Streaming (Aggregation-First)")
    print("   - Target Year: 2025\n")
    
    # 1. Setup
    out_dir = "exports"
    os.makedirs(out_dir, exist_ok=True)
    
    try:
        ingestor = DataIngestor()
        con = ingestor.con
        con.execute("SET memory_limit = '1GB';")
        
        # 2. Ingestion & Imputation
        print("\n" + "="*60)
        print("📥 PHASE 1: DATA INGESTION & IMPUTATION")
        print("="*60)
        ingestor.run_full_2025_ingestion()
        
        print("\n⬇️  Fetching 2024 Q1 comparison data...")
        path_24_q1 = ingestor.download_file('yellow', 2024, 1)
        
        print("\n🔗 Unifying schemas across Yellow and Green taxis...")
        ingestor.create_unified_view()
        print("✓ Schema unification complete!")
        
        # 3. Forensic Audit
        print("\n" + "="*60)
        print("🔍 PHASE 2: FORENSIC AUDIT")
        print("="*60)
        audit = AuditEngine(con)
        audit.create_forensic_audit()
        
        print("\n📊 Generating audit reports...")
        leakage = audit.calculate_leakage()
        leakage.to_csv(os.path.join(out_dir, "leakage_report.csv"), index=False)
        print(f"✓ Leakage report saved ({len(leakage)} locations)")
        
        decline_df = audit.compare_q1_volumes(path_24_q1)
        decline_df.to_csv(os.path.join(out_dir, "q1_decline.csv"), index=False)
        print(f"✓ Q1 volume comparison saved")
        
        suspicious_df = audit.get_suspicious_vendors()
        suspicious_df.to_csv(os.path.join(out_dir, "suspicious_vendors.csv"), index=False)
        print(f"✓ Suspicious vendors report saved ({len(suspicious_df)} entries)")
        
        # 4. Analytics Strategy
        print("\n" + "="*60)
        print("📈 PHASE 3: ANALYTICS & AGGREGATION")
        print("="*60)
        analytics = AnalyticsEngine(con, audit.CZ_ZONE_IDS)
        
        border_df = analytics.get_border_effect(path_24_q1)
        border_df.to_csv(os.path.join(out_dir, "border_effect.csv"), index=False)
        print(f"✓ Border Effect analysis saved ({len(border_df)} zones)")
        
        v24, v25 = analytics.get_velocity_comparison(path_24_q1)
        v24.to_csv(os.path.join(out_dir, "velocity_24.csv"), index=False)
        v25.to_csv(os.path.join(out_dir, "velocity_25.csv"), index=False)
        print(f"✓ Velocity heatmaps saved (2024: {len(v24)} points, 2025: {len(v25)} points)")
        
        tip_df = analytics.get_tip_crowding_analysis()
        tip_df.to_csv(os.path.join(out_dir, "tip_crowding.csv"), index=False)
        print(f"✓ Tip 'Crowding Out' analysis saved ({len(tip_df)} months)")
        
        rain_data, corr = analytics.get_rain_elasticity()
        if rain_data is not None:
            rain_data.to_csv(os.path.join(out_dir, "rain_data.csv"), index=False)
            with open(os.path.join(out_dir, "rain_stats.txt"), "w") as f:
                f.write(f"{corr:.4f}")
            print(f"✓ Rain elasticity saved (Correlation: {corr:.4f})")

        print("\n" + "="*60)
        print("✅ PIPELINE COMPLETE")
        print("="*60)
        print(f"\n📁 All exports saved to: {os.path.abspath(out_dir)}/")
        print("\n🚀 Next step: Run 'streamlit run app.py' to launch dashboard\n")
        return True
        
    except Exception as e:
        print("\n" + "="*60)
        print("❌ PIPELINE ERROR")
        print("="*60)
        print(f"Error: {str(e)}")
        import traceback
        print("\nFull traceback:")
        traceback.print_exc()
        print("\nPlease check internet connection and system resources.")
        return False

if __name__ == "__main__":
    run_production_pipeline()
