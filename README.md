# _Spark-Maven
A spark project with Java.

## Prerequisites
- [Java](https://java.com/en/download/)
- [Maven](https://maven.apache.org/)

## Build and Demo process

### Clone the Repo
`git clone https://github.com/ragnar-lothbrok/case-studies-solutions.git`

### Build
`mvn clean install`

### Running Spark Jobs

*/usr/local/spark-2.2.0-bin-hadoop2.6/bin/spark-submit --master yarn --deploy-mode cluster --class <class_name_with_package> <parameters>*

### Checking Spark Job Logs
*yarn logs -appOwner <Hadoop_user> -applicationId application_job_id*

###  Check job on yarn
http://<yarn_url>:8088/cluster

Make sure you create S3 bucket in US EAST 2(Oregon) only. There are some connecting issues with other regions.

Create Directory and upload Jar file and all CSV Files

*hadoop dfs -mkdir -p /data/data_csvs/*

To change permission of directory

*hadoop dfs -chown <user>:supergroup  /data/data_csvs/*


*/usr/local/spark/spark-2.2.0-bin-hadoop2.6/bin/spark-submit --master yarn --deploy-mode cluster --class  com.simplilearn.bigdata.january_casestudy_1.Solution_1 case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar <paths> yarn*

Steps to run in local

mvn clean install

For S3
==> Make sure you use region which is in code (use oregon). Else you won't be able to connect.

https://cloud.mongodb.com/
https://docs.mongodb.com/spark-connector/master/configuration/#spark-output-conf
https://docs.mongodb.com/spark-connector/master/scala/datasets-and-sql/#datatypes
https://stackoverflow.com/questions/51707267/mongodb-atlas-connection-not-working

==> It's totally free and can be used.
==> Once server is created
==> Go to Connect
==> Select Connect your application
==> Java and 3.4 or later
==> Use Mongo URI in your Application 

aws configure
export MONGOUSERNAME=
export MONGOPASSWORD=
export MONGOSERVER=
export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=


For Local 
*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.january_casestudy_1.Solution_1 /Users/labuser/Downloads/solution_1/city_attributes.csv /Users/labuser/Downloads/solution_1/pressure.csv /Users/labuser/Downloads/solution_1/humidity.csv /Users/labuser/Downloads/solution_1/temperature.csv /Users/labuser/Downloads/solution_1/weather_description.csv /Users/labuser/Downloads/solution_1/wind_direction.csv /Users/labuser/Downloads/solution_1/wind_speed.csv false false bucket local


*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.january_casestudy_1.KafkaProducer localhost:9092 city_topic /Users/labuser/Downloads/solution_1/city_attributes.csv

*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.january_casestudy_1.KafkaProducer localhost:9092 pressure_topic /Users/labuser/Downloads/solution_1/pressure.csv

*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.january_casestudy_1.KafkaProducer localhost:9092 humidity_topic /Users/labuser/Downloads/solution_1/humidity.csv

*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.january_casestudy_1.KafkaProducer localhost:9092 temperature_topic /Users/labuser/Downloads/solution_1/temperature.csv

*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.january_casestudy_1.KafkaProducer localhost:9092 weather_description_topic /Users/labuser/Downloads/solution_1/weather_description.csv

*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.january_casestudy_1.KafkaProducer localhost:9092 wind_direction_topic /Users/labuser/Downloads/solution_1/wind_direction.csv

*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.january_casestudy_1.KafkaProducer localhost:9092 wind_speed_topic /Users/labuser/Downloads/solution_1/wind_speed.csv

*java -cp case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar  com.simplilearn.bigdata.january_casestudy_3.KafkaProducer3 localhost:9092 category_topic US_category_id.json

*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.january_casestudy_3.KafkaProducer1 localhost:9092 category_topic /Users/labuser/Downloads/US_category_id.json


*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.january_casestudy_3.Solution_3 /Users/labuser/Downloads/category_title.csv /Users/labuser/Downloads/USvideos.csv false false solution3 local


*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.january_casestudy_2.Solution_2 /Users/labuser/Downloads/olist_public_dataset.csv false true ecommerce6915 local

hdfs dfs -put olist_public_dataset.csv  /tmp/


To run Flume
flume-ng agent -n kafka-flume  -f ../../../flume_conf/flume_kafka_city.conf


Headers
city_attributes.csv    City,Country
humidity.csv datetime,Vancouver,Portland,San Francisco,Seattle,Los Angeles,San Diego,Las Vegas,Phoenix,Albuquerque,Denver,San Antonio,Dallas,Houston,Kansas City,Minneapolis,Saint Louis,Chicago,Nashville,Indianapolis,Atlanta,Detroit,Jacksonville,Charlotte,Miami,Pittsburgh,Toronto,Philadelphia,New York,Montreal,Boston,Beersheba,Tel Aviv District,Eilat,Haifa,Nahariyya,Jerusalem
pressure.csv datetime,Vancouver,Portland,San Francisco,Seattle,Los Angeles,San Diego,Las Vegas,Phoenix,Albuquerque,Denver,San Antonio,Dallas,Houston,Kansas City,Minneapolis,Saint Louis,Chicago,Nashville,Indianapolis,Atlanta,Detroit,Jacksonville,Charlotte,Miami,Pittsburgh,Toronto,Philadelphia,New York,Montreal,Boston,Beersheba,Tel Aviv District,Eilat,Haifa,Nahariyya,Jerusalem
temperature.csv datetime,Vancouver,Portland,San Francisco,Seattle,Los Angeles,San Diego,Las Vegas,Phoenix,Albuquerque,Denver,San Antonio,Dallas,Houston,Kansas City,Minneapolis,Saint Louis,Chicago,Nashville,Indianapolis,Atlanta,Detroit,Jacksonville,Charlotte,Miami,Pittsburgh,Toronto,Philadelphia,New York,Montreal,Boston,Beersheba,Tel Aviv District,Eilat,Haifa,Nahariyya,Jerusalem
weather_description.csv datetime,Vancouver,Portland,San Francisco,Seattle,Los Angeles,San Diego,Las Vegas,Phoenix,Albuquerque,Denver,San Antonio,Dallas,Houston,Kansas City,Minneapolis,Saint Louis,Chicago,Nashville,Indianapolis,Atlanta,Detroit,Jacksonville,Charlotte,Miami,Pittsburgh,Toronto,Philadelphia,New York,Montreal,Boston,Beersheba,Tel Aviv District,Eilat,Haifa,Nahariyya,Jerusalem
wind_direction.csv datetime,Vancouver,Portland,San Francisco,Seattle,Los Angeles,San Diego,Las Vegas,Phoenix,Albuquerque,Denver,San Antonio,Dallas,Houston,Kansas City,Minneapolis,Saint Louis,Chicago,Nashville,Indianapolis,Atlanta,Detroit,Jacksonville,Charlotte,Miami,Pittsburgh,Toronto,Philadelphia,New York,Montreal,Boston,Beersheba,Tel Aviv District,Eilat,Haifa,Nahariyya,Jerusalem
wind_speed.csv datetime,Vancouver,Portland,San Francisco,Seattle,Los Angeles,San Diego,Las Vegas,Phoenix,Albuquerque,Denver,San Antonio,Dallas,Houston,Kansas City,Minneapolis,Saint Louis,Chicago,Nashville,Indianapolis,Atlanta,Detroit,Jacksonville,Charlotte,Miami,Pittsburgh,Toronto,Philadelphia,New York,Montreal,Boston,Beersheba,Tel Aviv District,Eilat,Haifa,Nahariyya,Jerusalem


Headers
olist_public_dataset.csv id,order_status,order_products_value,order_freight_value,order_items_qty,customer_city,customer_state,customer_zip_code_prefix,product_name_lenght,product_description_lenght,product_photos_qty,review_score,order_purchase_timestamp,order_aproved_at,order_delivered_customer_date

java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.january_casestudy_2.Solution_1 /Users/labuser/Downloads/olist_public_dataset.csv false false bucket local


Headers
category title csv = category, title
us videos csv = video_id,trending_date,title,channel_title,category_id,publish_time,tags,views,likes,dislikes,comment_count,comments_disabled,ratings_disabled


/Users/labuser/Downloads/category_title.csv /Users/labuser/Downloads/USvideos.csv false false bucket local


java -cp case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.TestMongo  solution1output


spark-submit --master yarn --deploy-mode cluster --class  com.simplilearn.bigdata.january_casestudy_2.Solution_2 case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar /tmp/olist_public_dataset.csv true true solution1output yarn


yarn application -list -appStates ALL

yarn logs -applicationId application_1581692054345_0001  -appOwner hadoop 

yarn application -kill application_1581692054345_0001

hdfs dfs -mkdir hdfs://ec2-3-20-223-171.us-east-2.compute.amazonaws.com/tmp/orders/

hdfs dfs -put <> hdfs://ec2-3-20-223-171.us-east-2.compute.amazonaws.com/tmp/orders/

hdfs dfs -put <> hdfs://ec2-3-20-223-171.us-east-2.compute.amazonaws.com/tmp/test/