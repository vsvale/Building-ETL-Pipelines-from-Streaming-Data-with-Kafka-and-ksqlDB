-- docker exec -it ksqldb-cli ksql http://ksqldb-server:8088

-- 1. Create raw readings stream
CREATE STREAM rawTempReadings (sensorID VARCHAR KEY, temp INTEGER)
  WITH (kafka_topic='RawTempReadings', key_format='KAFKA', value_format='KAFKA', partitions=1);

-- 5. Insert readings to aggregate
INSERT INTO rawTempReadings (sensorID, temp) VALUES ('abc123', -40);
INSERT INTO rawTempReadings (sensorID, temp) VALUES ('abc123', 32);
INSERT INTO rawTempReadings (sensorID, temp) VALUES ('abc123', 70); 

INSERT INTO rawTempReadings (sensorID, temp) VALUES ('xyz456', 60);
INSERT INTO rawTempReadings (sensorID, temp) VALUES ('xyz456', 65);
INSERT INTO rawTempReadings (sensorID, temp) VALUES ('xyz456', 70);


