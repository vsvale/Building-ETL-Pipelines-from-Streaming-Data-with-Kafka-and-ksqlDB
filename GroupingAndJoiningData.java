import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class GroupingAndJoiningData {

	
	public static void main(String[] args) {
		Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Weather_grouping");
        //Port 29092 is used because the docker compose file exposes that to our computer
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, Integer> rawReadings = builder.stream("RawTempReadings");
		@SuppressWarnings("unchecked") //Hides a data type warning for our branch code
		KStream<String, Integer>[] filteredReadings = rawReadings
				.branch((key, value) -> value > -50 && value < 130,
				        (key, value) -> true);
		
		KGroupedStream<String, Integer> gs = filteredReadings[1].groupByKey();
		KTable<String, Integer> errorCount = gs.count()
				.mapValues((value) -> value.intValue());
		
		KStream<String, Integer> validatedReadings = filteredReadings[0]
				.leftJoin( errorCount, 
						(leftvalue, rightvalue) -> 
							(rightvalue == null || rightvalue < 3) ? leftvalue : -99999);
		
		validatedReadings.to("ValidatedTempReadings");
		
		Topology topo = builder.build();
		KafkaStreams streams = new KafkaStreams(topo, props);
		streams.cleanUp();
		streams.start();
		
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
		System.out.println("Starting error counting");
	}

}
