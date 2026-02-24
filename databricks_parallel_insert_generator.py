from datetime import datetime, date
from decimal import Decimal

# ================================
# CONFIGURATION
# ================================
schema = "silver"
table = "silver_merchant"
full_table_name = f"{schema}.{table}"

# ================================
# FETCH DATA
# ================================
df = spark.table(full_table_name)

# ================================
# FUNCTION TO GENERATE INSERTS (runs in parallel on workers)
# ================================
def row_to_insert(row):
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

    cols = ", ".join(row.__fields__)
    vals = ", ".join(values)
    return f"INSERT INTO {schema}.{table} ({cols}) VALUES ({vals});"

# ================================
# EXECUTE IN PARALLEL → COLLECT STRINGS ONLY
# ================================
inserts = df.rdd.map(row_to_insert).collect()

print(f"-- INSERT INTO script for {full_table_name}")
print(f"-- Total rows: {len(inserts)}\n")

for stmt in inserts:
    print(stmt)
