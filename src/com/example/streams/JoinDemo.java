package com.example.streams;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;

public class JoinDemo {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9092,broker3:9092");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		System.out.println("Construct Streams!");

		StreamsBuilder builder = new StreamsBuilder();
		
		//streams the left topic
		KStream<String, String> leftStream = builder.stream("left-topic");
		
		//streams the right topic
		KStream<String, String> rightStream = builder.stream("right-topic");
		
		//join the value if the SAME KEY is found within 5 minutes window
		KStream<String, String> joinStream = leftStream.join(rightStream,
				
				//the join operation
				(leftValue, rightValue) -> leftValue + "-" + rightValue,
				
				//the time window
				JoinWindows.of(Duration.ofMinutes(5)));
		
		//output it to topic
		joinStream.to("joined-topic");
		
		//run the streams!
		KafkaStreams runStreams = new KafkaStreams(builder.build(), props);
		runStreams.start();
	}

}
