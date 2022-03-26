ADD JAR /home/saif/LFS/cohort_c9/jars/hive-hcatalog-core-3.1.0.jar;


set hive.exec.dynamic.partition.mode=nonstrict;

SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=1000;
create table review_bucket
(
review_id string,
user_id string,
business_id string,
useful int,
funny int,
cool int,
text string,
`date` string
)
partitioned by (stars integer)
clustered by (business_id) into 4 buckets;

insert overwrite table review_bucket partition (stars) select review_id,user_id,business_id,useful,funny,cool,text,`date`,stars from review1;


set hive.auto.convert.join=true;



create table business_bucket
(
address string,
business_id string,
categories string,
city string,
name string,
postal_code string,
review_count string,
stars double,
state string,
Review_status string
)
partitioned by (is_open string)
clustered by (business_id) into 4 buckets;



insert overwrite table business_bucket partition (is_open) select address,business_id,categories,city,name,postal_code,review_count,stars,state,Review_status,is_open from business;



set hive.auto.convert.join=true;

SELECT /*+ MAPJOIN(business_bucket) */ r.`date`,b.business_id,b.name,b.stars,b.Review_status,r.text
FROM business_bucket b JOIN review_bucket r ON b.business_id = r.business_id limit 5;


create table bus_rev as SELECT /*+ MAPJOIN(business_bucket) */ r.`date`,b.business_id,b.name,b.stars,b.Review_status,r.text FROM business_bucket b JOIN review_bucket r ON b.business_id = r.business_id;

