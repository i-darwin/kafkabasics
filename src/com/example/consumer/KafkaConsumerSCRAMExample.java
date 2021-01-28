package com.example.consumer;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumerSCRAMExample {

	public static void main(String[] args) {
		String bootstrapServer = "127.0.0.1:9093"; //your broker address, can also set hostname here instead of IP
		String topic = "TEST_TOPIC";
		String consumerGroupId = "example_consumer_group";
		String userName = "yourUserIdHere";
		String password = "yourPasswordHere";
		String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\""+userName+"\" password=\""+password+"\";";
		

		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    
    //SCRAM properties
		props.put("security.protocol", "SASL_PLAINTEXT");
    props.put("sasl.mechanism", "SCRAM-SHA-256");
    props.put("sasl.jaas.config", jaasTemplate);

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

		System.out.println(getDate() + "begin consumer: " + new Timestamp(new Date().getTime()));
		consumer.subscribe(Collections.singleton(topic));

		boolean alwaysPooling = true;

		while (alwaysPooling) {
			try {
				System.out.println(getDate() + "poll!");
				ConsumerRecords<String, String> dataFromKafka = consumer.poll(Duration.ofMinutes(5));

				if (dataFromKafka.count() == 0) {
					System.out.println(getDate() + "no data from pooling");
					continue;
				}

				dataFromKafka.forEach(data -> {
					System.out.println(getDate() + "Data from kafka: " + data.value());
				});

				System.out.println(getDate() + "commit offset");
				consumer.commitSync(); // commit offset here
				System.out.println(getDate() + "done commit offset");
			} catch (Exception ex) {
				System.out.println(getDate() + "ERROR!!");
				ex.printStackTrace();
				System.out.println(getDate() + "CONTINUE");
			}
		}

		System.out.println(getDate() + "close consumer: " + new Timestamp(new Date().getTime()));
		consumer.close();
	}

	static String getDate() {
		return new Timestamp(new Date().getTime()).toString() + " ";
	}

}
