from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Row

# define schema for our data using DDL
schema = "`Id` INT, `First` STRING, `Last` STRING, `Url` STRING,"\
"`Published` STRING, `Hits` INT, `Campaigns` ARRAY<STRING>"

# Create our static data
data = [[1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter", "LinkedIn"]],
       [2, "Brooke","Wenig","https://tinyurl.2", "5/5/2018", 8908, ["twitter", "LinkedIn"]],
       [3, "Denny", "Lee", "https://tinyurl.3","6/7/2019",7659, ["web", "twitter", "FB", "LinkedIn"]],
       [4, "Tathagata", "Das","https://tinyurl.4", "5/12/2018", 10568, ["twitter", "FB"]],
       [5, "Matei","Zaharia", "https://tinyurl.5", "5/14/2014", 40578, ["web", "twitter", "FB", "LinkedIn"]],
       [6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568, ["twitter", "LinkedIn"]]
      ]

# main program
if __name__ == "__main__":
    spark = SparkSession.builder.appName("Example-3_6").getOrCreate()
    # create a df using the chema defined above
    blogs_df = spark.createDataFrame(data, schema)
    # show the df
    blogs_df.show()
    # print the schema used by Spark to process the df
    print(blogs_df.printSchema())
    print("=" * 80)
    print(blogs_df.schema)

    print("=" * 80)
    print(blogs_df.columns)
    print("=" * 80)
    #print(blogs_df.col("Id"))

    print("=" * 80)
    # Use an expression to compute a value
    blogs_df.select(expr("Hits * 2")).show(2)

    print("=" * 80)
    # or use col to compute value
    blogs_df.select(col("Hits") * 2).show(2)

    print("=" * 80)
    blogs_df.withColumn("Big Hitters", (expr("Hits > 10000"))).show()

    print("=" * 80)
    (blogs_df.withColumn("AuthorsId", (concat(expr("First"),
        expr("Last"), expr("Id"))))
        .select(col("AuthorsId"))
        .show(4))

#    print("=" * 80)
#    # Sort by column Id in descending order
#    blogs_df.sort(col("Id").desc).show()
#    # blogs_df.sort($"Id".desc).show()

    print("ROWS " * 10)
    blog_row = Row(6, "Reynold", "Xin", "https:lalala", 222333, "3/2/2015",
                    ["twitter", "LinkedIn"])
    print(blog_row[1])
    
    print("=" * 80)
    rows = [Row("Mat", "CA"), Row("Rey", "CA")]
    authors_df = spark.createDataFrame(rows, ["Authors", "State"])
    authors_df.show()

