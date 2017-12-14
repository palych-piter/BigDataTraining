use bgtraining;

DROP TABLE cities;
CREATE EXTERNAL TABLE IF NOT EXISTS cities(
	id String, 
	name String
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
location '/bgtraining/module3/cities';

