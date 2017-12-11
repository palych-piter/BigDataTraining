DROP DATABASE IF EXISTS bgtraining CASCADE;
CREATE DATABASE bgtraining;
USE bgtraining;

DROP TABLE flying_not_partitioned;
CREATE EXTERNAL TABLE IF NOT EXISTS flying_not_partitioned(
	Year STRING, 
	Month STRING, 
	DayofMonth STRING, 
	DayOfWeek STRING, 
	DepTime STRING, 
	CRSDepTime STRING, 
	ArrTime STRING, 
	CRSArrTime STRING, 
	UniqueCarrier STRING, 
	FlightNum STRING, 
	TailNum STRING, 
	ActualElapsedTime STRING, 
	CRSElapsedTime STRING, 
	AirTime STRING, 
	ArrDelay STRING, 
	DepDelay STRING, 
	Origin STRING, 
	Dest STRING, 
	Distance STRING, 
	TaxiIn STRING, 
	TaxiOut STRING, 
	Cancelled STRING, 
	CancellationCode STRING, 
	Diverted STRING, 
	CarrierDelay STRING, 
	WeatherDelay STRING, 
	NASDelay STRING, 
	SecurityDelay STRING, 
	LateAircraftDelay STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
location '/bgtraining/module2/flying'
tblproperties ("skip.header.line.count"="1");


	
DROP TABLE airports_not_partitioned;
CREATE EXTERNAL TABLE IF NOT EXISTS airports_not_partitioned(
	iata STRING,
	airport STRING,
	city STRING,
	state STRING,
	country STRING,
	lat STRING,
	long STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
location '/bgtraining/module2/airports'
tblproperties ("skip.header.line.count"="1");

DROP TABLE carriers_not_partitioned;
CREATE EXTERNAL TABLE IF NOT EXISTS carriers_not_partitioned(
	code STRING,
	description STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
location '/bgtraining/module2/carriers'
tblproperties ("skip.header.line.count"="1");
	
	