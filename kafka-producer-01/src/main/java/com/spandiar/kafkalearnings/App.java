package com.spandiar.kafkalearnings;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * My first Kafka Producer
 *
 */
public class App {

	private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
	private static final String TOPIC_ONE = "topicOne";
	private static Logger logger = LoggerFactory.getLogger(App.class);

	public static void main(String[] args) {

		Properties kafkaProperties = setProperties();
		ProducerRecord<String, String> record = createRecord();

		publish(kafkaProperties, record);

	}

	private static void publish(Properties kafkaProperties, ProducerRecord<String, String> record) {
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProperties);

		producer.send(record, new Callback() {

			public void onCompletion(RecordMetadata metadata, Exception exception) {

				if (null == exception) {
					logger.info("Producer successful: \n" + "Topic is: " + metadata.topic() + "\n" + "Partition: "
							+ metadata.partition() + "\n");

				} else {
					logger.error(exception.getMessage());
				}

			}

		});
		producer.close();
		producer.flush();
	}

	private static ProducerRecord<String, String> createRecord() {
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC_ONE,
				"hello there...this message is post the refactoring with callbacks and SLF4j");
		return record;
	}

	private static Properties setProperties() {
		Properties kafkaProperties = new Properties();
		kafkaProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		kafkaProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		kafkaProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return kafkaProperties;
	}

}
