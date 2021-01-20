package com.sproutloud.starter.stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.test.context.junit4.SpringRunner;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Runs integration test
 * 
 * @author rgupta
 *
 */
@RunWith(SpringRunner.class)
@SpringBootTest(properties = { "spring.datasource.url=jdbc:postgresql://localhost:26257/lm?prepareThreshold=0",
        "spring.datasource.username=sluser", "spring.datasource.password=sproutloud",
        "spring.datasource.platform=postgresql" }, webEnvironment = SpringBootTest.WebEnvironment.NONE)
class SegmentDataIngestionApplicationTests {

    /**
     * processor object to be used
     */
    @Autowired
    protected Processor processor;

    /**
     * MessageCollector object to be used
     */
    @Autowired
    protected MessageCollector collector;

    /**
     * mapper object to be used
     */
    @Autowired
    protected ObjectMapper mapper;

    /**
     * Tests if context is loaded
     */
    @Test
    void contextLoads() {
        assertNotNull(processor);
    }

    /**
     * Tests if the Transformed output is in the required format for the passed
     * input
     * 
     * @throws InterruptedException
     * @throws JsonParseException
     * @throws JsonMappingException
     * @throws IOException
     */
    @Test
    void test() throws InterruptedException, JsonParseException, JsonMappingException, IOException {
        List<String> segments = new ArrayList<>();
        segments.add("LI20070000000022");
        segments.add("LI20070000000023");

        Map<String, Object> input = new HashMap<>();
        input.put("target_db", "lm");
        input.put("account_id", "AC20060000000002");
        input.put("modified_by", "rgupta");
        input.put("created_by", "rgupta");
        input.put("modified_op", "I");
        input.put("locality_code", "SL_US");
        input.put("list_id", "LI20070000000014");
        input.put("recipient_id", "RC20070000150750");
        input.put("job_id", "JO20090000000267");
        input.put("segments", segments);

        processor.input().send(MessageBuilder.withPayload(mapper.writeValueAsString(input)).build());
        Message<?> msg = collector.forChannel(processor.output()).poll();
        JsonNode outNode = mapper.readTree((String) Objects.requireNonNull(msg).getPayload());
        Map<String, Object> output = mapper.convertValue(outNode, new TypeReference<Map<String, Object>>() {
        });
        System.err.println(output);
        assertNotNull(output);
    }

    /**
     * Tests if error is logged in case of invalid input
     * 
     * @throws JsonProcessingException when given object is not bale to convert to string.
     */
    @Test
    void testInvalidInput() throws JsonProcessingException {
        Map<String, Object> input = new HashMap<>();
        input.put("target_db", "lm2_dev");
        input.put("account_id", "AC20060000000002");
        input.put("job_id", "JO20090000000267");
        processor.input().send(MessageBuilder.withPayload(mapper.writeValueAsString(input)).build());
        Message<?> msg = collector.forChannel(processor.output()).poll();
        JsonNode outNode = mapper.readTree((String) Objects.requireNonNull(msg).getPayload());
        Map<String, Object> output = mapper.convertValue(outNode, new TypeReference<Map<String, Object>>() {
        });
        System.err.println(output);
        assertNotNull(output);
    }

}
