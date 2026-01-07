package com.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;

public class KafkaConsumerPlainExample {

	public static void main(String[] args) {
		String brokerServers = "brokerHostname1:9092,brokerHostname2:9092,brokerHostname3:9092";
		String topic = "TEST_TOPIC";
		String consumerGroupId = "test_consumer_group";

		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerServers);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

		System.out.println(getDate() + "begin consumer: " + new Timestamp(new Date().getTime()));
		consumer.subscribe(Collections.singleton(topic));

		boolean alwaysPooling = true;
		List<String> uniqueIdList = new ArrayList<String>();

		while (alwaysPooling) {
			try {
				System.out.println(getDate() + "poll!");
				ConsumerRecords<String, String> dataFromKafka = consumer.poll(Duration.ofMinutes(5));

				if (dataFromKafka.count() == 0) {
					System.out.println(getDate() + "no data from pooling");
					continue;
				}

				dataFromKafka.forEach(data -> {
					System.out.println(data.value());
				});

				System.out.println(getDate() + "commit offset");
				consumer.commitSync(); // commit offset here
				System.out.println(getDate() + "done commit offset");
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}

		System.out.println(getDate() + "close consumer: " + new Timestamp(new Date().getTime()));
		consumer.close();
	}

	static String getDate() {
		return new Timestamp(new Date().getTime()).toString() + " ";
	}

}
