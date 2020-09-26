package com.example.kafkaconsumer.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import com.example.kafkaconsumer.kafka.KafkaMessageListener;
import com.example.kafkaproducer.model.Message;

@Configuration
@EnableKafka
public class KafkaConsumerConfig {
	
	private static final int POLL_TIMEOUT = 3000;

	@Value("${kafka.hosts}")
	private String kafkaHosts;
	
	@Value("${kafka.groupid}")
	private String kafkaGroupId;
	
	@Value("${kafka.listener.threads}")
	private int kafkaListeners;
	
	@Value("${kafka.session.timeout}")
	private int kafkaSessionTimeout;
	
	@Bean
	public Map<String, Object> consumerConfigs(){
		Map<String, Object> props = new HashMap<String, Object>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHosts);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaGroupId);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, kafkaSessionTimeout);
		return props;
	}
	
	@Bean
	public ConsumerFactory<String, Message> consumerFactory(){
		return new DefaultKafkaConsumerFactory<>(consumerConfigs(),
				new StringDeserializer(), new JsonDeserializer<>(Message.class));
	}
	
	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, Message> kafkaListenerContainerFactory(){
		ConcurrentKafkaListenerContainerFactory<String, Message> factory = 
				new ConcurrentKafkaListenerContainerFactory<String, Message>();
		factory.setConsumerFactory(consumerFactory());
		factory.getContainerProperties().setPollTimeout(POLL_TIMEOUT);
		factory.setConcurrency(kafkaListeners);
		return factory;
	}
	
	@Bean
	public KafkaMessageListener kafkaMessageListner() {
		return new KafkaMessageListener();
	}
}
