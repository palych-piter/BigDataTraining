# Hive UDF Examples

This code accompanies [this article which walks through creating UDFs in Apache Hive][blog-post].

## Compile

```
mvn compile
```

## Test

```
mvn test
```

## Build
```
mvn assembly:single
```

## Run

```
%> hive
hive> ADD JAR /path/to/udfTest.jar;
hive> create temporary function userAgentUDF as 'com.epam.hive.udf.example.SimpleUDFExample';
hive> select userAgentUDF(UserAgent) from logs limit 10;

```






[blog-post]:http://blog.matthewrathbone.com/2013/08/10/guide-to-writing-hive-udfs.html