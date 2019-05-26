# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession

'''
{"first": "a_f", "last": "a_l", "age": 5}
{"first": "b_f", "last": "b_l", "age": 1}

必须切换到java 8工作
./spark-submit --master "local[*]" --executor-memory 8g --total-executor-cores 8 /Users/why/PycharmProjects/spark_demo/demo_01/spark_sql_demo.py
'''




# SparkSession in Spark 2.0 provides builtin support for Hive features including the ability to write queries using HiveQL, access to Hive UDFs
spark = SparkSession\
    .builder\
    .appName("first spark session")\
    .getOrCreate()

# create dataframe

df = spark.read.json("/Users/why/spark/learning-spark/demo.json")

df.show()

df.printSchema()

# only show one column
'''
select 用于表示展示哪些字段
filter是 过滤哪些数据
!!! 这些很像sql
'''
df.select("first").show()

df.select("first", "age").show()

df.select("first", "age", df['age'] > 3).show()

df.select("first", "age").filter(df['age'] == 5).show()

df.groupBy("first").count().show()

# Register the DataFrame as a SQL temporary view 然后就可以sql操作了
# 创建一个people的 table
df.createOrReplaceTempView("people")

# 这个生成的也是一个dataframe
sqlDf = spark.sql("select first from people where age > 3")

sqlDf.show()


rows = sqlDf.rdd.collect()
for row in rows:
    print row

# 上面那个view 是和session绑定，还可以创建一个glbal的

'''
    datasets 只需要在scala java中考虑
'''

# rdd 与dataframe 交互




