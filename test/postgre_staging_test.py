from datetime import datetime
from pyspark.sql import Row

from utils.singletons import Spark

spark = Spark().get_spark_session()

base_table_name = 'test'

current_date = datetime.now().strftime("%Y_%m_%d")
staging_table_name = f"_staging_{base_table_name}_{current_date}"

data = [
    Row(id=1, name="Alice"),
    Row(id=2, name="Bob"),
    Row(id=3, name="Catherine")
]

df = spark.createDataFrame(data)

db_schema = 'test'

schema = df.schema
columns = []

for field in schema.fields:
    # Map PySpark data types to PostgreSQL types
    if field.dataType.typeName() == 'string':
        columns.append(f"{field.name} VARCHAR")
    elif field.dataType.typeName() == 'integer':
        columns.append(f"{field.name} INTEGER")
    elif field.dataType.typeName() == 'double':
        columns.append(f"{field.name} DOUBLE PRECISION")
    elif field.dataType.typeName() == 'date':
        columns.append(f"{field.name} DATE")
    # Add more data types as needed

create_table_query = f"""
CREATE TABLE IF NOT EXISTS {db_schema}.{staging_table_name} (
    {', '.join(columns)}
);
"""

if __name__=='__main__':
    print(create_table_query)