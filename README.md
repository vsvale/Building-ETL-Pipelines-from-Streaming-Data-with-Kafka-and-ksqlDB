# Building-ETL-Pipelines-from-Streaming-Data-with-Kafka-and-ksqlDB

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