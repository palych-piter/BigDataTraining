use bgtraining; 

add jar ../udf/udfTest.jar;
create temporary function userAgentUDF as 'com.matthewrathbone.example.SimpleUDFExample';

drop table if exists logs_parsed;
create table logs_parsed as  
select ret[0] as browser, ret[1] as browser_version, ret[2] as operating_system, cityID 
from ( select UserAgentUDF(UserAgent) as ret, cityID from logs ) tmp;
