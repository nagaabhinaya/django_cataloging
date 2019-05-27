
from pyspark.sql import SparkSession


def read_csv():
    try:
        #initializing sparkSession of pyspark
        spark = SparkSession.builder.master("local").appName("cataloging").config("spark.redis.host","localhost").config("spark.redis.port","6379").config("spark.jars","/Volumes/Official/Abhinaya/project/shoe-catalog/bin/spark-redis-master/target/spark-redis-2.4.0-SNAPSHOT-jar-with-dependencies.jar").getOrCreate()

        #reading the given csv file
        catalog_data = spark.read.format('csv').options(header='true', inferSchema='true').load('Datafiniti_Womens_Shoes.csv')
        #catalog_data.show()

        #building a dataframe with specific columns alone
        specific_data = catalog_data.select("id","dateAdded","dateUpdated","brand","colors")
        #specific_data.show(2)

        #writign date to redis
        specific_data.write.format("org.apache.spark.sql.redis").option("table", "shoe_catalog").option("key.column", "id").mode("overwrite").save()

    except Exception as e:
        print(e)
