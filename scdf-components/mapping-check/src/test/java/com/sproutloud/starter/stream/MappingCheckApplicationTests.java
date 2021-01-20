package com.sproutloud.starter.stream;

import static org.junit.Assert.assertNotNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Integration tests for AlfMappingCheckApplication.
 *
 * @author mgande
 *
 */
@SpringBootTest(
        properties = {
                "spring.cloud.gcp.credentials.location=${GCP_CREDENTIALS_LOCATION}",
                "spring.main.banner-mode=off",
                "spring.datasource.url=jdbc:postgresql://localhost:26257/lm?prepareThreshold=0",
                "spring.datasource.username=sluser",
                "spring.datasource.password=sproutloud",
                "spring.datasource.platform=postgresql",
        },
        webEnvironment = SpringBootTest.WebEnvironment.NONE)
class MappingCheckApplicationTests {

    @Autowired
    private Processor processor;

    @Autowired
    private ObjectMapper mapper;

    @Autowired
    private MessageCollector collector;

    /**
     * Tests the success case of mapping check.
     * 
     * @throws InterruptedException    if interrupted while polling the response
     * @throws JsonProcessingException if unable to parse response
     */
    @Test
	void testAlfMappingCheckSuccess() throws InterruptedException, JsonProcessingException {
	    String input = "{\n" + 
	            "    \"integration_id\" : \"IP20090000000002\",  \n" + 
	            "    \"list_id\": \"LI20090000000004\",\n" + 
	            "    \"account_id\": \"AC20060000000002\",\n" + 
	            "    \"target_db\": \"lm\",\n" + 
	            "    \"target_table\": \"list_data_ac20060000000002\",\n" + 
	            "    \"job_id\": \"SJ20200000000056\",\n" + 
	            "    \"user\": \"sluser\",\n" + 
	            "    \"job_type\": \"ingestion\",\n" + 
	            "    \"file\": \"gs://sl-dev-listmgmt/alf/test-pipeline-3.csv\",\n" + 
	            "    \"contains_headers\": \"true\",\n" + 
	            "    \"recipient_source\": \"CS20070000000012\",\n" + 
	            "    \"lease_table\": \"lease_data_ac20060000000002\",\n" + 
	            "    \"segment_table\": \"list_set_data_ac20060000000002\",\n" + 
	            "    \"dedupe_fields\": [\n" + 
	            "        \"first_name\",\n" + 
	            "        \"email\"\n" + 
	            "    ],\n" + 
	            "    \"is_cass_required\": \"true\",\n" + 
	            "    \"country_code\": \"USA\"\n" + 
	            "}";
	    processor.input().send(MessageBuilder.withPayload(input).build());
	    Message<?> message = collector.forChannel(processor.output()).poll(2, TimeUnit.SECONDS);
	    JsonNode jsonNode = mapper.readTree((String) Objects.requireNonNull(message).getPayload());
	    Map<String, Object> output = mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
        });
	    System.err.println(mapper.writeValueAsString(output));
	    assertNotNull(output);
	}

}
