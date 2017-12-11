use bgtraining;

SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;

DROP TABLE airports;
CREATE EXTERNAL TABLE IF NOT EXISTS airports(
	iata STRING,
	airport STRING,
	city STRING,
	state STRING,
	lat STRING,
	long STRING
)
PARTITIONED BY (country STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
location '/bgtraining/module2/airports_partitioned'
tblproperties ("skip.header.line.count"="1");

INSERT OVERWRITE TABLE airports PARTITION (country) 
SELECT iata, airport, city, state, lat,	long, country FROM airports_not_partitioned; 


DROP TABLE flying;
CREATE EXTERNAL TABLE IF NOT EXISTS flying(
	Year STRING, 
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
PARTITIONED BY (month STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
location '/bgtraining/module2/flying_partitioned'
tblproperties ("skip.header.line.count"="1");

INSERT OVERWRITE TABLE flying PARTITION (month) 
SELECT 
	Year, 
	DayofMonth, 
	DayOfWeek, 
	DepTime, 
	CRSDepTime, 
	ArrTime, 
	CRSArrTime, 
	UniqueCarrier, 
	FlightNum, 
	TailNum, 
	ActualElapsedTime, 
	CRSElapsedTime, 
	AirTime, 
	ArrDelay, 
	DepDelay, 
	Origin, 
	Dest, 
	Distance, 
	TaxiIn, 
	TaxiOut, 
	Cancelled, 
	CancellationCode, 
	Diverted, 
	CarrierDelay, 
	WeatherDelay, 
	NASDelay, 
	SecurityDelay, 
	LateAircraftDelay, 
        month
FROM flying_not_partitioned; 



