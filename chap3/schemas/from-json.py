import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: from-jason <file path to blogs.json>", file=sys.stderr)
        sys.exit(-1)

    json_file = sys.argv[1]
    schema = "`Id` INT, `First` STRING, `Last` STRING, `Url` STRING,"\
    "`Published` STRING, `Hits` INT, `Campaigns` ARRAY<STRING>"

    spark = (SparkSession.builder.appName("Example-3_7").getOrCreate())
    blogs_df = spark.read.schema(schema).json(json_file)
    
    blogs_df.show()
    print(blogs_df.printSchema())
    print(blogs_df.schema)

#if __name__ == "__main__":
#    spark = (SparkSession.builder
#                .appName("Example-3_6")
#                .getOrCreate())
#    # create a df using the chema defined above
#    blogs_df = spark.createDataFrame(data, schema)
#    # show the df
#    blogs_df.show()
#    # print the schema used by Spark to process the df
#    print(blogs_df.printSchema())
#    print("=" * 80)
#    print(blogs_df.schema)
#    
