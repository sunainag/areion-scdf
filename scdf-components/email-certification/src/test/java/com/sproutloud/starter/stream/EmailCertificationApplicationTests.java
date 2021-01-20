package com.sproutloud.starter.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

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
 * 
 * Runs the integration tests for Email Certification Application.
 * 
 * @author mgande
 *
 */
@SpringBootTest
class EmailCertificationApplicationTests {

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
        EmailCertificationApplication.main(new String[] {});
    }

    /**
     * Integration tests for the email certification of the incoming Message.
     * 
     * @throws InterruptedException    if interrupted while polling the response
     * @throws JsonProcessingException if unable to parse response
     */
    @Test
    void testPerformEmailCertification() throws InterruptedException, JsonProcessingException {
        String input = "{\n" +
                "  \"country_code\": \"USA\",\n" +
                "  \"account_id\": \"AC20070000000013\",\n" +
                "  \"fields_data\": {\n" +
                "    \"country\": \"USA\",\n" +
                "    \"contact_source\": \"CS20200000000001\",\n" +
                "    \"list_id\": \"LI20080000000508\",\n" +
                "    \"city\": \"MERCERSBURG\",\n" +
                "    \"locality_code\": \"SL_US\",\n" +
                "    \"email_message\": \"\",\n" +
                "    \"govt_id\": \"ABC000100\",\n" +
                "    \"sms_message\": \"\",\n" +
                "    \"company\": \"\",\n" +
                "    \"state\": \"PA\",\n" +
                "    \"first_name\": \"Swope\",\n" +
                "    \"email\": \"c100@test.com\",\n" +
                "    \"sms_status\": \"\",\n" +
                "    \"recipient_id\": \"RC20080000006116\",\n" +
                "    \"zip\": \"17236-1315\",\n" +
                "    \"modified_op\": \"I\",\n" +
                "    \"address2\": \"\",\n" +
                "    \"address1\": \"31 LOCUST DR\",\n" +
                "    \"last_name\": \"Nathan\",\n" +
                "    \"middle_name\": \"D\",\n" +
                "    \"email_status\": \"\",\n" +
                "    \"created_by\": \"sluser\",\n" +
                "    \"job_id\": \"SJ20080000000969\",\n" +
                "    \"mail_status\": \"\",\n" +
                "    \"modified_by\": \"sluser\",\n" +
                "    \"mail_message\": \"\"\n" +
                "  },\n" +
                "  \"is_cass_required\": \"false\",\n" +
                "  \"field_details\": {\n" +
                "    \"zip\": {\n" +
                "      \"is_required\": \"f\",\n" +
                "      \"data_type\": \"TEXT\",\n" +
                "      \"field_type\": \"DEFAULT\",\n" +
                "      \"field_format\": \"UPPERCASE\"\n" +
                "    },\n" +
                "    \"country\": {\n" +
                "      \"is_required\": \"f\",\n" +
                "      \"data_type\": \"TEXT\",\n" +
                "      \"field_type\": \"DEFAULT\",\n" +
                "      \"field_format\": \"UPPERCASE\"\n" +
                "    },\n" +
                "    \"govt_id\": {\n" +
                "      \"is_required\": \"f\",\n" +
                "      \"data_type\": \"TEXT\",\n" +
                "      \"field_type\": \"USER\",\n" +
                "      \"field_format\": \"UNFORMATTED\"\n" +
                "    },\n" +
                "    \"address2\": {\n" +
                "      \"is_required\": \"f\",\n" +
                "      \"data_type\": \"TEXT\",\n" +
                "      \"field_type\": \"DEFAULT\",\n" +
                "      \"field_format\": \"UPPERCASE\"\n" +
                "    },\n" +
                "    \"city\": {\n" +
                "      \"is_required\": \"f\",\n" +
                "      \"data_type\": \"TEXT\",\n" +
                "      \"field_type\": \"DEFAULT\",\n" +
                "      \"field_format\": \"UPPERCASE\"\n" +
                "    },\n" +
                "    \"address1\": {\n" +
                "      \"is_required\": \"f\",\n" +
                "      \"data_type\": \"TEXT\",\n" +
                "      \"field_type\": \"DEFAULT\",\n" +
                "      \"field_format\": \"UPPERCASE\"\n" +
                "    },\n" +
                "    \"last_name\": {\n" +
                "      \"is_required\": \"f\",\n" +
                "      \"data_type\": \"TEXT\",\n" +
                "      \"field_type\": \"DEFAULT\",\n" +
                "      \"field_format\": \"PROPERCASE\"\n" +
                "    },\n" +
                "    \"company\": {\n" +
                "      \"is_required\": \"f\",\n" +
                "      \"data_type\": \"TEXT\",\n" +
                "      \"field_type\": \"DEFAULT\",\n" +
                "      \"field_format\": \"UNFORMATTED\"\n" +
                "    },\n" +
                "    \"state\": {\n" +
                "      \"is_required\": \"f\",\n" +
                "      \"data_type\": \"TEXT\",\n" +
                "      \"field_type\": \"DEFAULT\",\n" +
                "      \"field_format\": \"UPPERCASE\"\n" +
                "    },\n" +
                "    \"middle_name\": {\n" +
                "      \"is_required\": \"f\",\n" +
                "      \"data_type\": \"TEXT\",\n" +
                "      \"field_type\": \"DEFAULT\",\n" +
                "      \"field_format\": \"PROPERCASE\"\n" +
                "    },\n" +
                "    \"first_name\": {\n" +
                "      \"is_required\": \"t\",\n" +
                "      \"data_type\": \"TEXT\",\n" +
                "      \"field_type\": \"DEFAULT\",\n" +
                "      \"field_format\": \"PROPERCASE\"\n" +
                "    },\n" +
                "    \"email\": {\n" +
                "      \"is_required\": \"f\",\n" +
                "      \"data_type\": \"TEXT\",\n" +
                "      \"field_type\": \"DEFAULT\",\n" +
                "      \"field_format\": \"UNFORMATTED\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"target_table\": \"list_data_ac20070000000013\",\n" +
                "  \"dedupe_fields\": [\n" +
                "    \"first_name\",\n" +
                "    \"email\"\n" +
                "  ],\n" +
                "  \"email_certification\": \"required\",\n" +
                "  \"phone_certification\": \"not_required\",\n" +
                "  \"target_db\": \"lm2_dev\",\n" +
                "  \"address_certification\": \"not_required\"\n" +
                "}";
        processor.input().send(MessageBuilder.withPayload(input).build());
        Message<?> response = collector.forChannel(processor.output()).poll(60, TimeUnit.SECONDS);
        JsonNode jsonNode = mapper.readTree((String) Objects.requireNonNull(response).getPayload());
        Map<String, Object> fields = mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
        });
        Map<String, Object> fieldsData = mapper.convertValue(fields.get("fields_data"), new TypeReference<Map<String, Object>>() {
        });
        assertNotNull(fields);
        assertNotNull(fieldsData);
        assertEquals("done", fields.get("email_certification"));
        assertEquals("VALID", fieldsData.get("email_status"));
    }

    /**
     * Tests if status is set to INVALID in case domain is given wrong.
     * 
     * @throws InterruptedException    if interrupted while polling the response
     * @throws JsonProcessingException if unable to parse response
     */
    @Test
    void testCertifyEmailInValidDomain() throws InterruptedException, JsonProcessingException {
        String input = "{\n" + "  \"fields_data\": {\n"
                + "    \"email\": \"man@gmail.comcdshgcvgshdvcgdvcgsdvchgdvschgvdshgcvsdhgvchdsgvchdgvchgvsdhcgvdsgcvdsgvcgdghcvdshgvchgdv\"\n"
                + "  }\n" + "}";
        processor.input().send(MessageBuilder.withPayload(input).build());
        Message<?> response = collector.forChannel(processor.output()).poll(60, TimeUnit.SECONDS);
        JsonNode jsonNode = mapper.readTree((String) Objects.requireNonNull(response).getPayload());
        Map<String, Object> fields = mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
        });
        Map<String, Object> fieldsData = mapper.convertValue(fields.get("fields_data"), new TypeReference<Map<String, Object>>() {
        });
        assertNotNull(fields);
        assertNotNull(fieldsData);
        assertEquals("failed", fields.get("email_certification"));
        assertEquals("INVALID", fieldsData.get("email_status"));
    }

    /**
     * Tests if status is set to INVALID in email provided is not valid.
     * 
     * @throws InterruptedException    if interrupted while polling the response
     * @throws JsonProcessingException if unable to parse response
     */
    @Test
    void testCertifyEmailInvalid() throws InterruptedException, JsonProcessingException {
        String input = "{\n" + "  \"fields_data\": {\n" + "    \"email\": \"mangmail.com\"\n" + "  }\n" + "}";
        processor.input().send(MessageBuilder.withPayload(input).build());
        Message<?> response = collector.forChannel(processor.output()).poll(60, TimeUnit.SECONDS);
        JsonNode jsonNode = mapper.readTree((String) Objects.requireNonNull(response).getPayload());
        Map<String, Object> fields = mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
        });
        Map<String, Object> fieldsData = mapper.convertValue(fields.get("fields_data"), new TypeReference<Map<String, Object>>() {
        });
        assertNotNull(fields);
        assertNotNull(fieldsData);
        assertEquals("failed", fields.get("email_certification"));
        assertEquals("INVALID", fieldsData.get("email_status"));
    }

    /**
     * Tests if status is set to NO_VALUE in case email is empty.
     * 
     * @throws InterruptedException    if interrupted while polling the response
     * @throws JsonProcessingException if unable to parse response
     */
    @Test
    void testCertifyEmailNOValue() throws InterruptedException, JsonProcessingException {
        String input = "{\n" + "  \"fields_data\": {\n" + "    \"email\": \"\"\n" + "  }\n" + "}";
        processor.input().send(MessageBuilder.withPayload(input).build());
        Message<?> response = collector.forChannel(processor.output()).poll(60, TimeUnit.SECONDS);
        JsonNode jsonNode = mapper.readTree((String) Objects.requireNonNull(response).getPayload());
        Map<String, Object> fields = mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
        });
        Map<String, Object> fieldsData = mapper.convertValue(fields.get("fields_data"), new TypeReference<Map<String, Object>>() {
        });
        assertNotNull(fields);
        assertNotNull(fieldsData);
        assertEquals("failed", fields.get("email_certification"));
        assertEquals("NO_VALUE", fieldsData.get("email_status"));

        input = "{\n" + "  \"fields_data\": {\n" + "    \"email\": \"   \"\n" + "  }\n" + "}";
        processor.input().send(MessageBuilder.withPayload(input).build());
        response = collector.forChannel(processor.output()).poll(60, TimeUnit.SECONDS);
        jsonNode = mapper.readTree((String) Objects.requireNonNull(response).getPayload());
        fields = mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
        });
        fieldsData = mapper.convertValue(fields.get("fields_data"), new TypeReference<Map<String, Object>>() {
        });
        assertNotNull(fields);
        assertNotNull(fieldsData);
        assertEquals("failed", fields.get("email_certification"));
        assertEquals("NO_VALUE", fieldsData.get("email_status"));
    }

    /**
     * Tests if status is set to NO_VALUE in case email field is not present.
     * 
     * @throws InterruptedException    if interrupted while polling the response
     * @throws JsonProcessingException if unable to parse response
     */
    @Test
    void testCertifyEmailNoEmailField() throws InterruptedException, JsonProcessingException {
        String input = "{\n" + "  \"fields_data\": {\n" + "    \"email1\": \"mangmail.com\"\n" + "  }\n" + "}";
        processor.input().send(MessageBuilder.withPayload(input).build());
        Message<?> response = collector.forChannel(processor.output()).poll(60, TimeUnit.SECONDS);
        JsonNode jsonNode = mapper.readTree((String) Objects.requireNonNull(response).getPayload());
        Map<String, Object> fields = mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
        });
        Map<String, Object> fieldsData = mapper.convertValue(fields.get("fields_data"), new TypeReference<Map<String, Object>>() {
        });
        assertNotNull(fields);
        assertNotNull(fieldsData);
        assertEquals("failed", fields.get("email_certification"));
        assertEquals("NO_VALUE", fieldsData.get("email_status"));
    }
    
    /**
     * Tests if status is set to NO_VALUE in case email field is not present.
     * 
     * @throws InterruptedException    if interrupted while polling the response
     * @throws JsonProcessingException if unable to parse response
     */
    @Test
    void testCertifyEmailNoFieldsData() throws InterruptedException, JsonProcessingException {
        String input = "{\n" + "  \"fields_data1\": {\n" + "    \"email1\": \"mangmail.com\"\n" + "  }\n" + "}";
        processor.input().send(MessageBuilder.withPayload(input).build());
        Message<?> response = collector.forChannel(processor.output()).poll(60, TimeUnit.SECONDS);
        JsonNode jsonNode = mapper.readTree((String) Objects.requireNonNull(response).getPayload());
        Map<String, Object> fields = mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
        });
        Map<String, Object> fieldsData = mapper.convertValue(fields.get("fields_data"), new TypeReference<Map<String, Object>>() {
        });
        assertNotNull(fields);
        assertNull(fieldsData);
    }

}
