package com.sproutloud.starter.stream;

import static org.junit.Assert.assertNotNull;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@RunWith(SpringRunner.class)
@SpringBootTest(properties = { "spring.datasource.url=jdbc:postgresql://localhost:26257/lm2_dev?prepareThreshold=0",
        "spring.datasource.username=sluser", "spring.datasource.password=sproutloud",
        "spring.datasource.platform=postgresql" }, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@DirtiesContext
class DeduplicationApplicationTests {

    @Autowired
    Processor processor;

    @Autowired
    MessageCollector collector;

    @Autowired
    ObjectMapper mapper;

    @Test
    void contextLoads() {
        assertNotNull(processor);
    }

    @Test
    void test() throws InterruptedException, IOException {
        String input = "{\n" +
                "  \"account_id\": \"AC20070000000013\",\n" +
                "  \"fields_data\": {\n" +
                "    \"country\": \"USA\",\n" +
                "    \"contact_source\": \n" +
                "      \"CS20200000000001\"\n" +
                "    ,\n" +
                "    \"list_id\": \"LI20080000000508\",\n" +
                "    \"city\": \"CARLISLE\",\n" +
                "    \"locality_code\": \"SL_US\",\n" +
                "    \"email_message\": \"VALID email address.\",\n" +
                "    \"govt_id\": \"ABC000070\",\n" +
                "    \"sms_message\": \"\",\n" +
                "    \"company\": \"\",\n" +
                "    \"state\": \"PA\",\n" +
                "    \"first_name\": \"Barcus\",\n" +
                "    \"email\": \"c70@test.com\",\n" +
                "    \"sms_status\": \"\",\n" +
                "    \"recipient_id\": \"RC20080000006605\",\n" +
                "    \"zip\": \"17013-4418\",\n" +
                "    \"modified_op\": \"I\",\n" +
                "    \"address2\": \"\",\n" +
                "    \"address1\": \"10 STRAWBERRY DR\",\n" +
                "    \"last_name\": \"Nathan\",\n" +
                "    \"middle_name\": \"E\",\n" +
                "    \"email_status\": \"VALID\",\n" +
                "    \"created_by\": \"sluser\",\n" +
                "    \"job_id\": \n" +
                "      \"SJ20080000000982\"\n" +
                "    ,\n" +
                "    \"mail_status\": \"\",\n" +
                "    \"modified_by\": \"sluser\",\n" +
                "    \"mail_message\": \"\",\n" +
                "    \"dedupe_hash\": \"D843FF98B0D903413B29ECBB730CE498\"\n" +
                "  },\n" +
                "  \"target_table\": \"list_data_ac20070000000013\",\n" +
                "  \"dedupe_fields\": [\n" +
                "    \"first_name\",\n" +
                "    \"email\"\n" +
                "  ],\n" +
                "  \"target_db\": \"lm2_dev\"\n" +
                "}";

        processor.input().send(MessageBuilder.withPayload(input).build());
        Message<?> response = collector.forChannel(processor.output()).poll(60, TimeUnit.SECONDS);
        JsonNode jsonNode = mapper.readTree((String) Objects.requireNonNull(response).getPayload());
        Map<String, Object> output = mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
        });
        assertNotNull(output.get("router_flag"));
    }
}
