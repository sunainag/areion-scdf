package com.example.multioutputprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;

import java.io.IOException;

@SpringBootApplication
@EnableBinding(MultiOutputProcessor.class)
public class MultiOutputProcessorApplication {

	private static final Log LOGGER = LogFactory.getLog(MultiOutputProcessorApplication.class);

	@Autowired
	protected MultiOutputProcessor channels;

	public static void main(String[] args) {
		SpringApplication.run(MultiOutputProcessorApplication.class, args);
	}

	@StreamListener(target = Processor.INPUT)
	public void ingestFileAndTransform(Message<String> msg) throws IOException {
		LOGGER.info("*********hello*******************");
		channels.segment().send(new GenericMessage<String>("output lease"));
		channels.lease().send(new GenericMessage<String>("output segment"));
	}

}
