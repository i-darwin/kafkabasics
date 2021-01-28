package com.example.streams;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueMapper;

public class FilterDemo {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-filter");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092,127.0.0.2:9092,127.0.0.3:9092");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		System.out.println("Construct Streams!");
		
		StreamsBuilder builder = new StreamsBuilder();
		
		//stream the topic
		KStream<String, String> filterStreams = builder.stream("raw_input_topic");
		
		//define how the message to be filtered - Lambda annotation, Java 8 style
		filterStreams.filter((key, value) -> value.contains("btpn"))
		
		//define how the message to be filtered - Java 7 Style
		/*filterStreams.filter(new Predicate<String, String>() {

			@Override
			public boolean test(String key, String value) {
				return value.contains("btpn");
			}
		})*/
		
		//output it to topic
		.to("filtered_output_topic");
		
		System.out.println("Begin Streams!");
		
		//streams won't start until you told it to
		KafkaStreams flowingStreams = new KafkaStreams(builder.build(), props);
		flowingStreams.start(); //only close during error handling

	}

}
