 🚖 NYC 2025 Congestion Pricing Forensic Audit

An optimized Big Data solution designed to process 100M+ rows of NYC TLC trip records on consumer-grade hardware. This project identifies the "Border Effect" of the 60th St tolling zone and analyzes the economic elasticity of driver tips and traffic velocity.

## 🏗️ Architecture Overview
This project follows the *"Aggregation-First"* design pattern, ensuring that heavy lifting is done within the database layer to maintain a minimal memory footprint.



### 🛠️ Key Engineering Features:
* *Memory-Safe ETL:* Orchestrated via DuckDB to process 15GB+ datasets within a 1GB RAM constraint.
* *Weighted Imputation:* Sophisticated 0.3/0.7 weighted sampling to synthesize missing data points while maintaining seasonal/weekly alignment.
* *Physics-Based Data Quality:* SQL-driven audit layers to filter "Ghost Trips," teleporters, and stationary fare anomalies.
* *Multi-Source Integration:* Joins NYC TLC transit data with NOAA meteorological data for weather elasticity modeling.
