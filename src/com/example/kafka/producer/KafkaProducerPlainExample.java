package com.example.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerPlainExample {

	public static void main(String[] args) {
		String bootstrapServer = "127.0.0.1:9092,127.0.0.2:9092,127.0.0.2:9092";
		String topic = "TEST_TOPIC";
		String message = "Hello World ";
//		String message = "{\"jsonKey\":\"jsonValue\",\"jsonKeyExample\":\"jsonValueExample\",\"jsonObjectKeyExample\":{\"jsonKeyInside\":\"jsonValueInside\"}}";

		// construct kafka credential
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10000);
		props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
		props.put(ProducerConfig.RETRIES_CONFIG, 2); //for auto retry
		props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
		props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
		// props.put(ProducerConfig.ACKS_CONFIG, "all"); //optional, value can be 0, 1, or -1, refer to kafka documentation

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

		for (int i = 0; i < 10; i++) {
			try {
				boolean isFail = false;
				int attempt = 0;
				ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, message + i);
				try {
					doSend(producer, record);
				} catch (ExecutionException e) {
					System.out.println("ERROR!!!");
					e.printStackTrace();
					
					//manual resend here
					isFail = true;
					while (isFail && attempt <= 3) {
						attempt++;
						try {
							System.out.println("RETRYING!");
							doSend(producer, record);
							break;
						} catch (ExecutionException e1) {
							System.out.println("RETRY ATTEMPT " + attempt + " ERROR");
							e1.printStackTrace();
							Thread.sleep(5000); //manual retry backoff
							continue;
						}
					}
				}
				Thread.sleep(1000); //pause so you can see the output
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		producer.flush();
		producer.close();

	}

	static void doSend(KafkaProducer<String, String> producer, ProducerRecord<String, String> record)
			throws InterruptedException, ExecutionException {
		System.out.println("sending: " + record.value());
		producer.send(record).get();
		System.out.println("success======================================");
	}

}
