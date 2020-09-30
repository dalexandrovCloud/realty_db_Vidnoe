import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import explode
import sqlite3
import pandas

spark = SparkSession.builder.appName('move_ru parse').getOrCreate()
filePath = sys.argv[1]+'/result_01.csv'
conn = sqlite3.connect('movedatabase.db')

# загрузка данных из файла
df = spark.read.csv(filePath, inferSchema=True, header=False)

# очистка полученных данных
split_column = F.split(df['_c2'], ' ')
df = df.withColumnRenamed('_c0', 'flat_id') \
    .withColumn('city_type', F.regexp_replace(split_column.getItem(2), r'[^а-я]', '')) \
    .withColumn('city', F.split(df['_c2'], '/').getItem(1)) \
    .withColumn('rooms', F.split(df['_c5'], ':').getItem(2)) \
    .withColumn('m2', F.split(df['_c5'], ' ').getItem(2)) \
    .withColumn('price', F.regexp_replace(F.col('_c1'), r'\D', '')) \
    .withColumn('price_m', F.round((F.col('price') / F.col('m2')), 0)) \
    .withColumn('m2_room', F.regexp_replace(F.split(df['_c5'], ':').getItem(1), r'м2 Комнат', '')) \
    .withColumn('floor', F.split(df['_c5'], ' ').getItem(0)) \
    .withColumn('region', split_column.getItem(0)) \
    .withColumn('highway', F.split(df['_c3'], ' ').getItem(0)) \
    .withColumn('mkad_km', F.split(df['_c4'], ' ').getItem(0)) \
    .withColumnRenamed('_c6', 'update_date') \
    .drop('_c1', '_c2', '_c3', '_c4', '_c5')
    # .write.csv(sys.argv[1]+'/result_clean', header = True)

# запись полученных данных в базу данных
pd_df = df.toPandas()   
pd_df.to_sql('flat_00', con=conn, if_exists='replace')

