Databricks Parallel INSERT Script Generator
A lightweight Databricks utility that generates SQL INSERT INTO statements from any Delta/Spark table — built to fully leverage Spark's parallel execution engine across worker nodes.

Why This Exists
When migrating data between environments (e.g., from a Data Lakehouse to a relational database like PostgreSQL, SQL Server, or MySQL), you often need raw SQL INSERT INTO scripts to seed or replicate data without relying on connectors or ETL pipelines.
Most naive implementations collect all rows to the driver node first, which:

Defeats the purpose of a distributed system
Causes memory bottlenecks on large tables
Makes the process slow and fragile

This tool solves that by generating INSERT statements in parallel across Spark executors, collecting only the final strings — not the raw data — to the driver.

How It Works:
Delta/Spark Table --> df.rdd.map(row_to_insert) runs in parallel across workers --> .collect() only lightweight strings come to driver --> Print / Save INSERT statements
  
Each worker node processes its own partition of the data, converting rows to SQL strings independently. The driver only receives the final text output.



Configuration
At the top of the script, set your target table:
pythonschema = "silver"
table  = "silver_merchant"
That's it. The script auto-detects column names and data types.

Supported Data Types
Python / Spark TypeSQL Output ExampleNoneNULLboolTRUE / FALSEstr'value' (escaped '')datetime / date'2024-01-15 10:30:00'Decimal123.45numeric types42

How to Use
In Databricks Notebook

Attach the script to a cluster with access to your target table.
Update schema and table at the top.
Run the cell.
Copy the printed output or redirect it to a file.

Save Output to a File (optional)
pythonoutput = "\n".join(inserts)
dbutils.fs.put("/mnt/your-path/insert_script.sql", output, overwrite=True)

Common Use Cases

Database migration — move data from Databricks Delta tables to a traditional RDBMS
Environment seeding — populate dev/test/staging databases with production data snapshots
Audit & replay — generate reproducible SQL scripts for compliance or debugging
Vendor delivery — hand off data to teams or clients who only have SQL access


Limitations & Considerations

Best suited for small to medium tables (up to a few million rows). For very large datasets, consider chunking by partition or using native connectors (JDBC, COPY INTO, etc.).
The output is printed to the driver console. For very large outputs, redirect to DBFS or cloud storage instead of printing.
SQL dialect is generic. You may need minor adjustments for database-specific syntax (e.g., INSERT OR REPLACE for SQLite, UPSERT for PostgreSQL).
