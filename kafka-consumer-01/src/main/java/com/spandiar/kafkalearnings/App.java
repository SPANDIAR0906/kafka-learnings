package com.spandiar.kafkalearnings;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * My first Kafka Producer
 *
 */
public class App {

	private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
	private static final String TOPIC_ONE = "topicOne";
	private static final String CONSUMER_GROUP = "MAIN";
	private static Logger logger = LoggerFactory.getLogger(App.class);

	public static void main(String[] args) {

		Properties kafkaProperties = setProperties();
		consume(kafkaProperties);

	}

	private static void consume(Properties kafkaProperties) {
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(kafkaProperties);
		
		consumer.subscribe(Arrays.asList(TOPIC_ONE));
		
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			
			for(ConsumerRecord<String,  String> record : records) {
				logger.info("Details are " + "topic " + record.topic() + " partition " + record.partition() + " key " + record.key() + " value " + record.value());
			}
		}
		
	}


	private static Properties setProperties() {
		Properties kafkaProperties = new Properties();
		kafkaProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		kafkaProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		kafkaProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		kafkaProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
		kafkaProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return kafkaProperties;
	}

}
