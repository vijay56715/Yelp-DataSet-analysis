from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType,StringType,IntegerType
from pyspark.sql.functions import explode,split,from_json,struct,create_map,when




spark=SparkSession.builder.getOrCreate()

#Reading review and business datasets

df_business=spark.read.json("file:///home/saif/LFS/cohort_c9/yelp/yelp_academic_dataset_business.json")

df_review=spark.read.json("file:///home/saif/LFS/cohort_c9/yelp/yelp_academic_dataset_review.json")

#df_business.show()
#df_business.printSchema()
# print(df_business.count())

#Creating a extra columns for review status for analysis and cleaning data(dropping null values and duplicates)


df1_business=df_business.withColumn("Review_status",when(df_business.stars < 4.0 ,"Bad").otherwise("Good"))
#df1.show()
# print(df1_business.distinct().count())
df2_business=df1_business.dropna().limit(5000)

# print(df_review.count())
df1_review=df_review.dropna().limit(10000)
df2_business.show()
df2_business.printSchema()

#Writing the cleaned and sample datasets to hadoop

# print(df1_review.count())
df2_business.write.json("hdfs://localhost:9000/user/saif/HFS/output/project/business",mode="overwrite")
df1_review.write.json("hdfs://localhost:9000/user/saif/HFS/output/project/review/",mode="overwrite")
