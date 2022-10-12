# [Building-ETL-Pipelines-from-Streaming-Data-with-Kafka-and-ksqlDB](https://app.pluralsight.com/library/courses/kafka-streams-ksql-fundamentals)

Kafka can do more than just storing streaming data. This course will teach you to aggregate and analyze data in Kafka with ksqlDB and Kafka Streams. 

## Author

[Eugene Meidinger](https://app.pluralsight.com/profile/author/eugene-meidinger)

## Learning Platform

[Pluralsight](https://www.pluralsight.com/)

## Description
There is plenty of value in learning how to turn Kafka into a stream analytics engine. In this course, Building ETL Pipelines from Streaming Data with Kafka and KSQL, you’ll learn to shape and transform your Kafka streaming data. First, you’ll explore how ksqlDB and Kafka Streams both solve this problem. Next, you’ll discover how to transform your streams. Finally, you’ll learn how to aggregate and enrich data. When you’re finished with this course, you’ll have the skills and knowledge of ksqlDB and Kafka Streams needed to extract insights from Kafka streaming data.

## Notes

### Install KSQLDB
- https://ksqldb.io/quickstart.html
- docker compose pull
- docker compose up
- docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
- SHOW ALL TOPICS;

### Config KSQLDB
SET 'auto.offset.reset'='earliest';

### Push query
CREATE STREAM tempReadings (zipcode VARCHAR, sensortime BIGINT, temp DOUBLE)
    WITH(kafka_topic='readings',timestamp='sensortime',value_format='json, partitions=1');

SELECT zipcode, TIMESTAMPTOSTRING(WINDOWSTART,'HH:mm:ss') AS windowtime, AVG(temp) as temp
FROM tempReadings
WINDOW TUMBLING (SIZE 1 HOURS)
GROUP BY zipcode
EMIT CHANGES;

### Pull Query
CREATE TABLE highsandlows WITH (kafka_topic='readings') AS
    SELECT MIN(temp) as min_temp, max(temp) as max_temp, zipcode
    FROM tempReadings GROUP BY zipcode;

SELECT min_temp, max_temp, zipcode
FROM highsandlows
WHERE zipcode= 25005;

### Join
SELECT tr.temp,
CASE WHEN tr.temp - hsl.min_temp <= 5 THEN 'Low'
WHEN hsl.max_temp - tr.temp <= 5 THEN 'High'
ELSE 'Normal' END AS classification
FROM tempReadings tr
LEFT JOIN highsandlows hsl ON tr.zipcode = hsl.zipcode
EMIT CHANGES;