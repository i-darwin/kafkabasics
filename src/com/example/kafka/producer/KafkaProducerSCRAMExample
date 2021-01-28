package com.example.kafka.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerSCRAMExample {

	public static void main(String[] args) {

		String bootstrapServer = "kafkahostname1:9093,kafkahostname2:9093,kafkahostname3:9093"; //can use hostname or IP of broker here
		String topic = "TEST_TOPIC";
		String message = "Hello World ";

		String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"yourSCRAMUsernameHere\" password=\"yourSCRAMPasswordHere\";";
		
		// construct kafka credential
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10000);
		props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
		props.put(ProducerConfig.RETRIES_CONFIG, 2);
		props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
		props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
    
    //for SCRAM
		props.put("security.protocol", "SASL_PLAINTEXT"); //in this example, the broker security is SASL_PLAINTEXT
    props.put("sasl.mechanism", "SCRAM-SHA-256"); //in this example, the SASL is set using SHA-256
    props.put("sasl.jaas.config", jaasTemplate);

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
					
					//resend here
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
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
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
		System.out.println("=========================================");
	}

}
