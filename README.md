## Description

scala-2 is a spark application that process aicraft log files (from http://stat-computing.org/dataexpo/2009/) and print on the screen the results of the queries below.

1. Count total number of flights per carrier in 2007.
2. The total number of flights served in Jun 2007 by NYC.
3. Find five most busy airports in US during Jun 01 - Aug 31.
4. Find the carrier who served the biggest number of flights.

## Testing environment

Program was tested on HDP 2.4 sandbox.

## How to deploy

1. Make scala-2-assembly-1.0.jar by running sbt command from project root:
```
sbt clean assembly
```
2. Copy scala-2-assembly-1.0.jar from target/scala-2.10/ to machine with Spark installed.
3. Download dataset (http://stat-computing.org/dataexpo/2009/the-data.html year 2007 + dimensions http://stat-computing.org/dataexpo/2009/supplemental-data.html only Airports and Carrier Codes) and put it to hdfs.
4. Run Spark job, defining cluster manager and hdfs paths to dataset files:
```
spark-submit \
--class aircraft.App \
--master <cluster_manager> \
<path_to_jar>/scala-2-assembly-1.0.jar \
--airports <hdfs_path_to_airports_dataset>/airports.csv \
--carriers <hdfs_path_to_carriers_dataset>/carriers.csv \
--flights <hdfs_path_to_flights_dataset>/2007.csv.bz2
```
4.1 Local mode task submitting may look like this:
```
spark-submit \
--class aircraft.App \
--master local[*] \
scala-2-assembly-1.0.jar \
--airports /training/hive/airports/airports.csv \
--carriers /training/hive/carriers/carriers.csv \
--flights /training/hive/flights/2007.csv.bz2
```
4.2 Submitting to YARN example:
```
spark-submit \
--class aircraft.App \
--master yarn \
--executor-memory 512m \
--driver-memory 512m \
scala-2-assembly-1.0.jar \
--airports /training/hive/airports/airports.csv \
--carriers /training/hive/carriers/carriers.csv \
--flights /training/hive/flights/2007.csv.bz2
```
**It is recommended to define --executor-memory and --driver-memory flags when running on YARN. Otherwise your job may wait forever for resources allocation**

## Results

You can find screenshots of the queries execution in /screenshots folder.