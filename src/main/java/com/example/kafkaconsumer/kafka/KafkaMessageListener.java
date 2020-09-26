package com.example.kafkaconsumer.kafka;

import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;

import com.example.kafkaproducer.model.Message;

@KafkaListener(id = "my-listener", topics="MyTopic")
public class KafkaMessageListener {

	 @KafkaHandler
	 public void onMessage(Message message) {
			System.out.println("Kafka Listener Reading Message : " + message);
	 }
}
