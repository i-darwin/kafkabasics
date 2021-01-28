package com.example.consumer;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumeSeekPartition {

	public static void main(String[] args) {
		String brokerServers = "127.0.0.1:9092";
		String topic = "TEST_TOPIC";
		String consumerGroupId = "example_consumer_group";

		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerServers);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		TopicPartition tp0 = new TopicPartition(topic, 0); //keep making these variables for every partition you want to assign manually
		TopicPartition tp1 = new TopicPartition(topic, 1); //in this example, we want this consumer to consume from partition 0, partition 1, and partition 2
		TopicPartition tp2 = new TopicPartition(topic, 2); //keep in mind that other consumer in the same consumer group won't be assigned to these partitions anymore

		System.out.println(getDate() + "begin consumer: " + new Timestamp(new Date().getTime()));

		consumer.subscribe(Collections.singleton(topic), new ConsumerRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				// Move to the desired start
				consumer.seek(tp0, 123L); //for partition 0, we want to consume starting from offset 123
				consumer.seek(tp1, 54L); //the offset number must be in Long format
				consumer.seek(tp2, 10000L);
			}
		});

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
						System.out.println(getDate() + "Offset: " + data.offset());
						System.out.println(getDate() + "Data from kafka: " + data.value());
				});

				consumer.commitSync(); // commit offset here
				Thread.sleep(3000); //just to throttle so you can observe the result
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
