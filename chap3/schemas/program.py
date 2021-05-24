# programmatically
from spark.sql.types import *

schema = StructType([StructField('author', StringType(), False),
                    StructField('title', StringType(), False),
                    StructField('pages', IntegerType(), False)])
