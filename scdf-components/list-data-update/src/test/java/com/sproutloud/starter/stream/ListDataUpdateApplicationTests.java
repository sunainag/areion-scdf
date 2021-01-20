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
 * Integration tests for ListDataUpdateApplication.
 * 
 * @author mgande
 *
 */
@SpringBootTest(properties = { "spring.datasource.url=jdbc:postgresql://localhost:26257/lm?prepareThreshold=0",
        "spring.datasource.username=sluser", "spring.datasource.password=sproutloud",
        "spring.datasource.platform=postgresql" }, webEnvironment = SpringBootTest.WebEnvironment.NONE)
class ListDataUpdateApplicationTests {

    @Autowired
    private Processor processor;

    @Autowired
    private MessageCollector collector;

    @Autowired
    private ObjectMapper mapper;

    /**
     * Integration tests for the list data update success case.
     * 
     * @throws InterruptedException    if interrupted while polling the response
     * @throws JsonProcessingException if unable to parse response
     */
    @Test
    void testUpdateListData() throws InterruptedException, JsonProcessingException {
        String input = "{\n" + 
                "   \"segment_data\":{\n" + 
                "      \"target_table\":\"lease_data_ac20060000000015\",\n" + 
                "      \"segments\":[\n" + 
                "         \"LI20080000000991\",\n" + 
                "         \"LI20080000000992\",\n" + 
                "         \"LI20080000000998\",\n" + 
                "         \"LI20080000000999\"\n" + 
                "      ]\n" + 
                "   },\n" + 
                "   \"fields_data\":{\n" + 
                "      \"country\":\"USA\",\n" + 
                "      \"recipient_source\":\"CS20070000000004\",\n" + 
                "      \"list_id\":\"LI20080000000943\",\n" + 
                "      \"city\":\"MERCERSBURG\",\n" + 
                "      \"locality_code\":\"SL_US\",\n" + 
                "      \"email_message\":\"VALID email address.\",\n" + 
                "      \"govt_id\":\"ABC000003\",\n" + 
                "      \"sms_message\":\"\",\n" + 
                "      \"company\":\"\",\n" + 
                "      \"state\":\"PA\",\n" + 
                "      \"first_name\":\"Keefer\",\n" + 
                "      \"email\":\"c3@test.com\",\n" + 
                "      \"sms_status\":\"\",\n" + 
                "      \"recipient_id\":\"RC20080000008397\",\n" + 
                "      \"zip\":\"17236-9456\",\n" + 
                "      \"modified_op\":\"I\",\n" + 
                "      \"address2\":\"\",\n" + 
                "      \"address1\":\"11006 SYLVAN DR\",\n" + 
                "      \"mobile\":\"\",\n" + 
                "      \"last_name\":\"Martin\",\n" + 
                "      \"middle_name\":\"Donald\",\n" + 
                "      \"email_status\":\"VALID\",\n" + 
                "      \"created_by\":\"nyenugula\",\n" + 
                "      \"job_id\":\"SJ20080000001243\",\n" + 
                "      \"mail_status\":\"\",\n" + 
                "      \"modified_by\":\"nyenugula\",\n" + 
                "      \"mail_message\":\"\",\n" + 
                "      \"dedupe_hash\":\"24DFF48FE68C215BD459468D334982C2\"\n" + 
                "   },\n" + 
                "   \"number_of_records\":4,\n" + 
                "   \"target_db\":\"lm2_dev\",\n" + 
                "   \"lease_data\":{\n" + 
                "      \"target_table\":\"list_set_data_ac20060000000015\",\n" + 
                "      \"tp_accounts\":[\n" + 
                "         \"tp_acct16\",\n" + 
                "         \"tp_acct2\",\n" + 
                "         \"tp_acct12\",\n" + 
                "         \"tp_acct1\"\n" + 
                "      ]\n" + 
                "   },\n" + 
                "   \"account_id\":\"AC20060000000015\",\n" + 
                "   \"target_table\":\"list_data_ac20060000000015\",\n" + 
                "   \"dedupe_fields\":[\n" + 
                "      \"first_name\",\n" + 
                "      \"email\"\n" + 
                "   ],\n" + 
                "   \"router_flag\":\"UPDATE\",\n" + 
                "   \"db_fields\":{\n" + 
                "      \"country\":\"USA\",\n" + 
                "      \"recipient_source\":\"{CS20070000000004}\",\n" + 
                "      \"list_id\":\"LI20080000000943\",\n" + 
                "      \"city\":\"MERCERSBURG\",\n" + 
                "      \"locality_code\":\"SL_US\",\n" + 
                "      \"email_message\":\"VALID email address.\",\n" + 
                "      \"govt_id\":\"ABC000003\",\n" + 
                "      \"sms_message\":\"\",\n" + 
                "      \"company\":\"\",\n" + 
                "      \"state\":\"PA\",\n" + 
                "      \"first_name\":\"Keefer\",\n" + 
                "      \"email\":\"c3@test.com\",\n" + 
                "      \"sms_status\":\"\",\n" + 
                "      \"recipient_id\":\"RC20080000008389\",\n" + 
                "      \"zip\":\"17236-9456\",\n" + 
                "      \"modified_op\":\"I\",\n" + 
                "      \"address2\":\"\",\n" + 
                "      \"address1\":\"11006 SYLVAN DR\",\n" + 
                "      \"mobile\":\"\",\n" + 
                "      \"last_name\":\"Martin\",\n" + 
                "      \"middle_name\":\"Donald\",\n" + 
                "      \"email_status\":\"VALID\",\n" + 
                "      \"created_by\":\"nyenugula\",\n" + 
                "      \"dedupe_hash\":\"24DFF48FE68C215BD459468D334982C2\",\n" + 
                "      \"job_id\":\"{SJ20080000001217,SJ20080000001218}\",\n" + 
                "      \"mail_status\":\"\",\n" + 
                "      \"modified_by\":\"nyenugula\",\n" + 
                "      \"mail_message\":\"\"\n" + 
                "   }\n" + 
                "}";

        processor.input().send(MessageBuilder.withPayload(input).build());
        Message<?> response = collector.forChannel(processor.output()).poll(60, TimeUnit.SECONDS);
        JsonNode jsonNode = mapper.readTree((String) Objects.requireNonNull(response).getPayload());
        Map<String, Object> output = mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
        });
        assertNotNull(output);
    }

}
