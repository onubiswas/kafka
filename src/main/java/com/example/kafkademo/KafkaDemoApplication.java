package com.example.kafkademo;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@SpringBootApplication
public class KafkaDemoApplication {

	public static void main(String[] args) {
//		SpringApplication.run(KafkaDemoApplication.class);
		publish();
//		consumer();
	}
	public static void publish() {
		String bootstrapServers = "localhost:9092";
		String topic = "quickstart-events";

		// Configure the producer properties
		Properties properties = new Properties();
		properties.put("bootstrap.servers", bootstrapServers);
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		// Create the Kafka producer
		Producer<String, String> producer = new KafkaProducer<>(properties);

		try {
			// Produce a sample message
			String key = "key";
			String value = "{Hello, Everyone}";
			ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

			// Send the message to Kafka
			producer.send(record);
			System.out.println("Message sent to topic: " + topic);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// Close the producer
			producer.close();
		}
	}
	public static void consumer() {
		String bootstrapServers = "localhost:9092";
		String groupId = "sdjlgajslag";
		String topic = "quickstart-events";

		// Configure the consumer properties
		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

		// Create the Kafka consumer
		Consumer<String, String> consumer = new KafkaConsumer<>(properties);

		// Subscribe to the topic
		consumer.subscribe(Collections.singletonList(topic));

		try {
			// Poll for records in a loop
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

				// Process the records
				records.forEach(record -> {
					TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
					System.out.println("Consumer Group: " + groupId);
					System.out.println("Topic: " + record.topic());
					System.out.println("Partition: " + record.partition());
					System.out.println("Offset: " + record.offset());
					System.out.println("Key: " + record.key());
					System.out.println("Value: " + record.value());
				});
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// Close the consumer on exit
			consumer.close();
		}
	}
}
