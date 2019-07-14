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

/usr/local/spark-2.2.0-bin-hadoop2.6/bin/spark-submit --master yarn --deploy-mode cluster --class <class_name_with_package> <parameters>

### Checking Spark Job Logs
yarn logs -appOwner <Hadoop_user> -applicationId application_<job

###  Check job on yarn
http://<namenode_url>:8088/cluster


Steps to run in local

mvn clean install

java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar  com.simplilearn.bigdata.casestudy_13.Solution_1 /Users/raghugup/Downloads/CaseStudies/flights_graph_case_study_13.csv local
java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar  com.simplilearn.bigdata.casestudy_13.Solution_2 /Users/raghugup/Downloads/CaseStudies/flights_graph_case_study_13.csv local
java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar  com.simplilearn.bigdata.casestudy_13.Solution_3 /Users/raghugup/Downloads/CaseStudies/flights_graph_case_study_13.csv local


java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar  com.simplilearn.bigdata.casestudy_12.Solution_1 localhost:9092 transaction_data transaction_data local 5 5
java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar  com.simplilearn.bigdata.casestudy_12.Solution_2 localhost:9092 transaction_data transaction_data local 10 5
java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar  com.simplilearn.bigdata.casestudy_12.Solution_3 localhost:9092 transaction_data transaction_data local 10

java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar  com.simplilearn.bigdata.casestudy_10.Solution_1 /Users/raghugup/Downloads/CaseStudies/all_companies_details_case_study_10.csv local
java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar  com.simplilearn.bigdata.casestudy_10.Solution_2 /Users/raghugup/Downloads/CaseStudies/all_companies_details_case_study_10.csv local
java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar  com.simplilearn.bigdata.casestudy_10.Solution_3 /Users/raghugup/Downloads/CaseStudies/all_companies_details_case_study_10.csv local

java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar  com.simplilearn.bigdata.casestudy_7.Solution_1 /Users/raghugup/Downloads/CaseStudies/companies_case_study_7.csv
java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar  com.simplilearn.bigdata.casestudy_7.Solution_2 /Users/raghugup/Downloads/CaseStudies/companies_case_study_7.csv
java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar  com.simplilearn.bigdata.casestudy_7.Solution_3 /Users/raghugup/Downloads/CaseStudies/companies_case_study_7.csv

java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar  com.simplilearn.bigdata.casestudy_6.DataUtil /Users/raghugup/Downloads/CaseStudies/Traffic_accidents_by_time_of_occurrence_2001_2014_case_study_6.csv local <namenodes>
java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar  com.simplilearn.bigdata.casestudy_6.Solution_1 <namenodes>
java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar  com.simplilearn.bigdata.casestudy_6.Solution_2 <namenodes>
java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar  com.simplilearn.bigdata.casestudy_6.Solution_3 <namenodes>


java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.casestudy_4.solution_1.Solution <input> <output>
java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.casestudy_4.solution_2_1.Solution <input> <output> <caches_file>
java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.casestudy_4.solution_2_2.Solution <input> <output> <caches_file>
java -cp target/case-studies-solutions-1.0-SNAPSHOT-jar-with-dependencies.jar com.simplilearn.bigdata.casestudy_4.solution_3.Solution <input> <output>