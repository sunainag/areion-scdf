package com.example.multioutputprocessor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import static org.springframework.integration.IntegrationMessageHeaderAccessor.CORRELATION_ID;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class MultiOutputProcessorApplicationTests {

	private static final Log LOGGER = LogFactory.getLog(MultiOutputProcessorApplicationTests.class);
	@Autowired
	protected MultiOutputProcessor channels;

	@Autowired
	protected MessageCollector collector;

	@Autowired
	protected ObjectMapper mapper;

	@Test
	public 	void contextLoads() {
	}

	@org.junit.Test
	public void testProcessor(){
		channels.input()
						.send(MessageBuilder.withPayload("testing testing").setHeader(CORRELATION_ID, java.util.UUID.randomUUID()).build());
	}

}
