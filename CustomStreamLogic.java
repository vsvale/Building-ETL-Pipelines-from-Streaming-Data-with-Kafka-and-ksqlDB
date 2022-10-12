import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class CustomStreamLogic {

	public static void main(String[] args) {
		Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Weather_logic");
        //Port 29092 is used because the docker compose file exposes that to our computer
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Double().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, Double> rawReadings = builder.stream("RawTempReadings");
		KStream<String, Double> convertedReadings = rawReadings
				.peek((key,value) -> System.out.print("F: " + value))
				.mapValues((value) -> (value - 32) / 1.8)
				.peek((key,value) -> System.out.println(" / C: " + value));
		convertedReadings.to("ConvertedTempReadings");
		
		Topology topo = builder.build();
		
		KafkaStreams streams = new KafkaStreams(topo, props);
		streams.cleanUp();
		streams.start();
		
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

	}

}
