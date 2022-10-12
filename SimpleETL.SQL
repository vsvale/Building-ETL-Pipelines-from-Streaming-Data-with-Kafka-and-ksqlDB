-- docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
-- 1. Create raw readings stream
CREATE STREAM rawTempReadings (sensorID VARCHAR KEY, temp INTEGER)
  WITH (kafka_topic='RawTempReadings', key_format='KAFKA', value_format='KAFKA', partitions=1);

-- 2. Create validated readings table
CREATE STREAM validatedTempReadings (sensorID VARCHAR KEY, temp INTEGER)
  WITH (kafka_topic='ValidatedTempReadings', key_format='KAFKA', value_format='KAFKA', partitions=1);

-- 3. Create streaming query for filtered results
SELECT *
FROM validatedTempReadings EMIT CHANGES;

-- 4. Split new window
-- docker exec -it ksqldb-cli ksql http://ksqldb-server:8088

-- 5. Insert filterable results
INSERT INTO rawTempReadings (sensorID, temp) VALUES ('abc123', 50);

INSERT INTO rawTempReadings (sensorID, temp) VALUES ('abc123', 2000);
INSERT INTO rawTempReadings (sensorID, temp) VALUES ('abc123', 20);


