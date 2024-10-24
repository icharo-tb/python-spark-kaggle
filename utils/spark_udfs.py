from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import uuid

def get_uuid():
    return str(uuid.uuid4())

uuid_udf = udf(get_uuid, StringType())