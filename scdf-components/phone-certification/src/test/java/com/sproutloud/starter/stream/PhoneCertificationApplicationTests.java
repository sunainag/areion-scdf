package com.sproutloud.starter.stream;

import static org.junit.Assert.assertEquals;
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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 *
 * Runs the integration tests for Phone Certification Application.
 *
 * @author mgande
 *
 */
@SpringBootTest
class PhoneCertificationApplicationTests {

    @Autowired
    protected Processor processor;

    @Autowired
    protected MessageCollector collector;

    @Autowired
    protected ObjectMapper mapper;

    /**
     * main method test.
     */
    @Test
    void main() {
        PhoneCertificationApplication.main(new String[] {});
    }

    /**
     * Integration tests for the phone certification of the incoming Message.
     *
     * @throws InterruptedException    if interrupted while polling the response
     * @throws JsonProcessingException if unable to parse response
     */
    @Test
    void testPerformPhoneCertification() throws InterruptedException, JsonProcessingException {
        String input = "{\n" +
                "    \"target_table\": \"list_data_ac20060000000002\",\n" +
                "    \"target_db\": \"lm2_dev\", \n" +
                "    \"account_id\": \"AC20060000000002\", \n" +
                "    \"fields_data\": {\n" +
                "        \"first_name\": \"Naveen\",\n" +
                "        \"email\": \"nave@tes.cd\",\n" +
                "        \"address1\": \"AFF\",\n" +
                "        \"address2\": \"AEE\",\n" +
                "        \"city\": \"HYY\",\n" +
                "        \"state\": \"JJU\",\n" +
                "        \"zip\": \"KKOI\",\n" +
                "        \"mobile\": \"875876986\",\n" +
                "        \"recipient_id\": \"RC20070000000001\",\n" +
                "        \"list_id\": \"LI20070000000014\",\n" +
                "        \"locality_code\": \"SL_US\",\n" +
                "        \"job_id\": \"SJ20200000000056\",\n" +
                "    \"contact_source\": [\n" +
                "      \"CS20200000000001\"\n" +
                "    ],\n" +
                "        \"created_by\": \"sluser\",\n" +
                "        \"modified_by\": \"sluser\",\n" +
                "        \"modified_op\": \"I\",\n" +
                "        \"email_status\": \"VALID\",\n" +
                "        \"email_message\": \"Valid Email address\",\n" +
                "        \"sms_status\": \"\",\n" +
                "        \"sms_message\": \"\",\n" +
                "        \"mail_status\": \"\",\n" +
                "        \"mail_message\": \"\"\n" +
                "    },\n" +
                "    \"lease_data\": {\n" +
                "        \"target_table\": \"lease_data_ac20060000000002\",\n" +
                "        \"tp_accounts\": [\n" +
                "            \"tp1\",\n" +
                "            \"tp2\"\n" +
                "        ]\n" +
                "    },\n" +
                "    \"segment_data\": {\n" +
                "        \"target_table\": \"list_set_data_ac20060000000002\",\n" +
                "        \"segments\": [\n" +
                "            \"LI20070000000012\",\n" +
                "            \"LI20070000000013\"\n" +
                "        ]\n" +
                "    },\n" +
                "    \"field_details\": {\n" +
                "        \"first_name\": {\n" +
                "            \"data_type\": \"TEXT\",\n" +
                "            \"field_type\": \"DEFAULT\",\n" +
                "            \"field_format\": \"PROPERTEXT\",\n" +
                "            \"is_required\": \"true\"\n" +
                "        },\n" +
                "        \"email\": {\n" +
                "            \"data_type\": \"TEXT\",\n" +
                "            \"field_type\": \"DEFAULT\",\n" +
                "            \"field_format\": \"UNFORMATTED\",\n" +
                "            \"is_required\": \"true\"            \n" +
                "        }\n" +
                "    },\n" +
                "    \"dedupe_fields\": [\n" +
                "        \"first_name\",\n" +
                "        \"email\"\n" +
                "    ],\n" +
                "    \"country_code\": \"USA\",\n" +
                "    \"is_cass_required\": true,\n" +
                "    \"email_certification\": \"done\",\n" +
                "    \"phone_certification\": \"required\",\n" +
                "    \"address_certification\": \"required\"\n" +
                "}\n" +
                "";
        processor.input().send(MessageBuilder.withPayload(input).build());
        Message<?> response = collector.forChannel(processor.output()).poll(60, TimeUnit.SECONDS);
        JsonNode jsonNode = mapper.readTree((String) Objects.requireNonNull(response).getPayload());
        Map<String, Object> fields = mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
        });
        Map<String, Object> fieldsData = mapper.convertValue(fields.get("fields_data"), new TypeReference<Map<String, Object>>() {
        });
        assertNotNull(fields);
        assertEquals("VALID", fieldsData.get("sms_status"));
        assertEquals("done", fields.get("phone_certification"));
    }

    /**
     * Integration tests for the phone certification of the incoming Message.
     *
     * @throws InterruptedException    if interrupted while polling the response
     * @throws JsonProcessingException if unable to parse response
     */
    @Test
    void testPerformPhoneCertificationValidWithCountry() throws InterruptedException, JsonProcessingException {
        String input = "{\n" + 
                "  \"fields_data\": {\n" + 
                "    \"mobile\": \"78887889\",\n" + 
                "    \"country\": \"USA\"\n" + 
                "  },\n" + 
                "  \"country_code\": \"USA\"\n" + 
                "}";
        processor.input().send(MessageBuilder.withPayload(input).build());
        Message<?> response = collector.forChannel(processor.output()).poll(60, TimeUnit.SECONDS);
        JsonNode jsonNode = mapper.readTree((String) Objects.requireNonNull(response).getPayload());
        Map<String, Object> fields = mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
        });
        Map<String, Object> fieldsData = mapper.convertValue(fields.get("fields_data"), new TypeReference<Map<String, Object>>() {
        });
        assertNotNull(fields);
        assertEquals("VALID", fieldsData.get("sms_status"));
        assertEquals("done", fields.get("phone_certification"));
    }
    /**
     * Test to check invalid input.
     *
     * @throws InterruptedException    if interrupted while polling the response
     * @throws JsonProcessingException if unable to parse response
     */
    @Test
    void testPerformPhoneCertificationInvalid() throws InterruptedException, JsonProcessingException {
        Map<String, Object> input = new HashMap<String, Object>();
        processor.input().send(MessageBuilder.withPayload(mapper.writeValueAsString(input)).build());
        Message<?> response = collector.forChannel(processor.output()).poll(60, TimeUnit.SECONDS);
        JsonNode jsonNode = mapper.readTree((String) Objects.requireNonNull(response).getPayload());
        Map<String, Object> fields = mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
        });
        
        assertEquals("failed", fields.get("phone_certification"));
    }
    
    /**
     * Test to check invalid case when no country and country code is given.
     *
     * @throws InterruptedException    if interrupted while polling the response
     * @throws JsonProcessingException if unable to parse response
     */
    @Test
    void testPerformPhoneCertificationNoCountry() throws InterruptedException, JsonProcessingException {
        String input = "{\n" + 
                "  \"fields_data\": {\n" + 
                "    \"mobile\": \"78897977\"\n" + 
                "  }\n" + 
                "}";
        processor.input().send(MessageBuilder.withPayload(input).build());
        Message<?> response = collector.forChannel(processor.output()).poll(60, TimeUnit.SECONDS);
        JsonNode jsonNode = mapper.readTree((String) Objects.requireNonNull(response).getPayload());
        Map<String, Object> fields = mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
        });
        Map<String, Object> fieldsData = mapper.convertValue(fields.get("fields_data"), new TypeReference<Map<String, Object>>() {
        });
        assertNotNull(fields);
        assertEquals("failed", fields.get("phone_certification"));
        assertEquals("INVALID", fieldsData.get("sms_status"));
    }
    
    /**
     * Test to check invalid case(INVALID).
     *
     * @throws InterruptedException    if interrupted while polling the response
     * @throws JsonProcessingException if unable to parse response
     */
    @Test
    void testPerformPhoneCertificationInvalidMobile() throws InterruptedException, JsonProcessingException {
        String input = "{\n" + 
                "  \"fields_data\": {\n" + 
                "    \"mobile\": \"78\",\n" + 
                "    \"country\": \"USA\"\n" + 
                "  }\n" + 
                "}";
        processor.input().send(MessageBuilder.withPayload(input).build());
        Message<?> response = collector.forChannel(processor.output()).poll(60, TimeUnit.SECONDS);
        JsonNode jsonNode = mapper.readTree((String) Objects.requireNonNull(response).getPayload());
        Map<String, Object> fields = mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
        });
        Map<String, Object> fieldsData = mapper.convertValue(fields.get("fields_data"), new TypeReference<Map<String, Object>>() {
        });
        assertNotNull(fields);
        assertEquals("failed", fields.get("phone_certification"));
        assertEquals("INVALID", fieldsData.get("sms_status"));
    }
    
    /**
     * Test to check invalid Phone(NO_VALUE).
     *
     * @throws InterruptedException    if interrupted while polling the response
     * @throws JsonProcessingException if unable to parse response
     */
    @Test
    void testPerformPhoneCertificationEmptyPhone() throws InterruptedException, JsonProcessingException {
        String input = "{\n" + 
                "  \"fields_data\": {\n" + 
                "    \"mobile\": \"\"\n" + 
                "  }\n" + 
                "}";
        processor.input().send(MessageBuilder.withPayload(input).build());
        Message<?> response = collector.forChannel(processor.output()).poll(60, TimeUnit.SECONDS);
        JsonNode jsonNode = mapper.readTree((String) Objects.requireNonNull(response).getPayload());
        Map<String, Object> fields = mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
        });
        Map<String, Object> fieldsData = mapper.convertValue(fields.get("fields_data"), new TypeReference<Map<String, Object>>() {
        });
        assertNotNull(fields);
        assertEquals("failed", fields.get("phone_certification"));
        assertEquals("NO_VALUE", fieldsData.get("sms_status"));
    }
    
    /**
     * Test to check invalid Phone(NO_VALUE).
     *
     * @throws InterruptedException    if interrupted while polling the response
     * @throws JsonProcessingException if unable to parse response
     */
    @Test
    void testPerformPhoneCertificationEmptyPhone1() throws InterruptedException, JsonProcessingException {
        String input = "{\n" + 
                "  \"fields_data\": {\n" + 
                "    \"mobile\": \"   \"\n" + 
                "  }\n" + 
                "}";
        processor.input().send(MessageBuilder.withPayload(input).build());
        Message<?> response = collector.forChannel(processor.output()).poll(60, TimeUnit.SECONDS);
        JsonNode jsonNode = mapper.readTree((String) Objects.requireNonNull(response).getPayload());
        Map<String, Object> fields = mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
        });
        Map<String, Object> fieldsData = mapper.convertValue(fields.get("fields_data"), new TypeReference<Map<String, Object>>() {
        });
        assertNotNull(fields);
        assertEquals("failed", fields.get("phone_certification"));
        assertEquals("NO_VALUE", fieldsData.get("sms_status"));
    }
    

}
