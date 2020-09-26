package com.example.kafkaproducer.model;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class Message {

	@JsonProperty("time_create")
	private Instant timeCreate;

}
