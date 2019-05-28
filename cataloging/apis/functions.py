from pyspark.sql import SparkSession
from pyspark.sql.functions import desc,col,to_date,lower
from pandas import DataFrame
from apis import fileRead

#Creating spark session
#spark = SparkSession.builder.master("local").appName("cataloging").config("spark.redis.host","localhost").config("spark.redis.port","6379").config("spark.jars","spark-redis-2.4.0-SNAPSHOT-jar-with-dependencies.jar").getOrCreate()

#Read date from redis
#df = fileRead.spark.read.format("org.apache.spark.sql.redis").option("table", "shoe_catalog").option("key.column", "id").load()

def get_recent_items(date_input):
    try:
        df = fileRead.spark.read.format("org.apache.spark.sql.redis").option("table", "shoe_catalog").option("key.column", "id").load()
        dateFrame = df.filter(to_date(col("dateAdded"),"yyyy-mm-dd").cast("date") == date_input).sort(desc("dateAdded")).limit(1)
        df1 = dateFrame.toPandas()
        j = df1.to_json(orient='records')
        return j
    except Exception as e:
        print(e)

def get_brand_count(date_input):
    try:
        df = fileRead.spark.read.format("org.apache.spark.sql.redis").option("table", "shoe_catalog").option("key.column", "id").load()
        dateFrame = df.filter(to_date(col("dateAdded"),"yyyy-mm-dd").cast("date") == date_input)
        brand_count = dateFrame.groupby("brand").agg({"id":"count"}).sort(desc("count(id)"))
        df1 = brand_count.toPandas()
        j = df1.to_json(orient='records')
        return j
    except Exception as e:
        print(e)

def get_latest_items_by_color(color):
    try:
        df = fileRead.spark.read.format("org.apache.spark.sql.redis").option("table", "shoe_catalog").option("key.column", "id").load()
        colorFrame = df.where(lower(col("colors")).like("%"+color+"%"))
        sorted_by_date = colorFrame.sort(desc("dateAdded")).limit(10)
        df1 = sorted_by_date.toPandas()
        j = df1.to_json(orient='records')
        return j
    except Exception as e:
        print(e)
