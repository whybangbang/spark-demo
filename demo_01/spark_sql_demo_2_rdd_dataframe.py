# -*- coding: utf-8 -*-

from pyspark.sql import Row
from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName("first spark session")\
    .getOrCreate()

sc = spark.sparkContext

lines = sc.textFile("/Users/why/spark/learning-spark/demo.txt")

lines_arr = lines.map(lambda x: x.split(","))

'''
# 这是最终准备处理的rdd
然后转换为df
df 到view
'''
people = lines_arr.map(lambda x: Row(first=x[0], last=x[1], age=x[2]))

people_df = spark.createDataFrame(people)

people_view = people_df.createOrReplaceTempView("people")

result_df = spark.sql("select * from people WHERE age > 4")

result_df.show()

# 还可以自己定义schema



