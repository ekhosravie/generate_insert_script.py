%python
from datetime import datetime, date
from decimal import Decimal

# ================================
# CONFIGURATION
# ================================
schema = "silver"
table = "silver_merchant"  # change this

# ================================
# GENERATE INSERT INTO SCRIPT
# ================================
full_table_name = f"{schema}.{table}"
df = spark.sql(f"SELECT * FROM {full_table_name}")
columns = df.columns
rows = df.collect()

print(f"-- INSERT INTO script for {schema}.{table}")
print(f"-- Total rows: {len(rows)}\n")

for row in rows:
    values = []
    for val in row:
        if val is None:
            values.append("NULL")
        elif isinstance(val, bool):
            values.append(str(val).upper())
        elif isinstance(val, str):
            escaped = val.replace("'", "''")
            values.append(f"'{escaped}'")
        elif isinstance(val, (datetime, date)):
            values.append(f"'{val}'")
        elif isinstance(val, Decimal):
            values.append(str(val))
        else:
            values.append(str(val))
    
    cols = ", ".join(columns)
    vals = ", ".join(values)
    print(f"INSERT INTO {schema}.{table} ({cols}) VALUES ({vals});")
