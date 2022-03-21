from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType,StringType,IntegerType
from pyspark.sql.functions import explode,count,split,from_json,lit,struct,create_map,col,when,avg
import  pandas as pd
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt


spark=SparkSession.builder.enableHiveSupport().getOrCreate()


spark.sql("use project")

df=spark.sql("select * from bus_rev")

#Analysis using   average and count 

df1=df.select("name","stars").groupBy("name").agg(avg("stars")).orderBy(avg("stars").desc()).limit(10)
df2=df.select("name","stars").groupBy("stars").agg(count("name"))

df3=df.select("name","stars").groupBy("name").agg(avg("stars")).orderBy(avg("stars")).limit(10)
df4=df.select("name","stars").groupBy("name").agg(count("stars"))
#df3=df.select("date","stars").orderBy("date")
#df2.show()

pdf1=df1.toPandas()
pdf2=df2.toPandas()
pdf3=df3.toPandas()
pdf4=df4.toPandas()

#Graphical Representation of Analysis

pdf1.plot(kind='bar',y='avg(stars)',x='name')

plt.show()

pdf3.plot(kind='bar',y='avg(stars)',x='name')

plt.show()

pdf2.plot(kind='bar',y='count(name)',x='stars')

plt.show()

pdf4.plot(kind='bar',y='count(stars)',x='name')

plt.show()