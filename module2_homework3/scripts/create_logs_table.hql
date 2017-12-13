use bgtraining;

DROP TABLE logs;
CREATE EXTERNAL TABLE IF NOT EXISTS logs(
	BidID STRING,
	Timestamp_ STRING,
	LogType STRING,
	iPinYouID STRING,
	UserAgent STRING,
	IP STRING,
	RegionID STRING,
	CityID STRING,
	AdExchange STRING,
	Domain STRING,
	URL STRING,
	AnonymousURL STRING,
	AdSlotID STRING,
	AdSlotWidth STRING,
	AdSlotHeight STRING,
	AdSlotVisibility STRING,
	AdSlotFormatFixed STRING,
	AdSlotFloorPrice STRING,
	CreativeID STRING,
	BiddingPrice STRING,
	PayingPrice STRING,
	LandingPageURL STRING,
	AdvertiserID STRING,
	UserProfileIDs STRING	
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
location '/bgtraining/module3/logs';

