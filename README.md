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


Create Directory and upload Jar file and all CSV Files

*hadoop dfs -mkdir -p /data/data_csvs/*

To change permission of directory

*hadoop dfs -chown <user>:supergroup  /data/data_csvs/*


*/usr/local/spark/spark-2.2.0-bin-hadoop2.6/bin/spark-submit --master yarn --deploy-mode cluster --class  com.simplilearn.bigdata.january_casestudy_1.Solution_1 case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar <paths> yarn*

Steps to run in local

mvn clean install

*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.january_casestudy_1.Solution_1 <data> local*

*java -cp build/libs/kafka-producer-1.0-SNAPSHOT.jar com.simplilearn.bigdata.january_casestudy_1.KafkaProducer localhost:9092 data_topic <files folder>


For S3
==> Make sure you use region which is in code. Else you won't be able to connect.

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

export MONGOUSERNAME=
export MONGOPASSWORD=

export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=


For Local 
*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.january_casestudy_1.Solution_1 /Users/labuser/Downloads/solution_1/city_attributes.csv /Users/labuser/Downloads/solution_1/pressure.csv /Users/labuser/Downloads/solution_1/humidity.csv /Users/labuser/Downloads/solution_1/temperature.csv /Users/labuser/Downloads/solution_1/weather_description.csv /Users/labuser/Downloads/solution_1/wind_direction.csv /Users/labuser/Downloads/solution_1/wind_speed.csv local


*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.january_casestudy_1.KafkaProducer localhost:9092 city_topic /Users/labuser/Downloads/solution_1/city_attributes.csv

*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.january_casestudy_1.KafkaProducer localhost:9092 pressure_topic /Users/labuser/Downloads/solution_1/pressure.csv

*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.january_casestudy_1.KafkaProducer localhost:9092 humidity_topic /Users/labuser/Downloads/solution_1/humidity.csv

*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.january_casestudy_1.KafkaProducer localhost:9092 temperature_topic /Users/labuser/Downloads/solution_1/temperature.csv

*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.january_casestudy_1.KafkaProducer localhost:9092 weather_description_topic /Users/labuser/Downloads/solution_1/weather_description.csv

*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.january_casestudy_1.KafkaProducer localhost:9092 wind_direction_topic /Users/labuser/Downloads/solution_1/wind_direction.csv

*java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.january_casestudy_1.KafkaProducer localhost:9092 wind_speed_topic /Users/labuser/Downloads/solution_1/wind_speed.csv
