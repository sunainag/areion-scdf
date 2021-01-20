package com.sproutloud.starter.stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import static com.sproutloud.starter.stream.StringUtils.ADDRESS_CERTIFICATION;
import static com.sproutloud.starter.stream.StringUtils.IS_CERTIFICATION_NOT_REQUIRED;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Runs integration tests
 * Tests the incoming message format
 *
 * @author sgoyal
 */
@SpringBootTest
class DataValidationsApplicationTests {

    @Autowired
    protected Processor channels;

    @Autowired
    protected MessageCollector collector;

    @Autowired
    protected ObjectMapper mapper;

    /**
     * Tests if context is loaded
     */
    @Test
    void contextLoads() {
        assertNotNull(channels);
    }

    /**
     * Tests when required fields are missing in the incoming Message, expected {@link RuntimeException}
     */
    @Test
    void testMissingFieldDetails() {
        String content = "{\n" +
                "    \"fields_data\": {\n" +
                "    },\n" +
                "}";
        assertThrows(RuntimeException.class, () -> channels.input().send(MessageBuilder.withPayload(content).build()));
    }

    /**
     * Tests for validating missing first_name and email in incoming Message, throws exception and skips record.
     */
    @Test
    void testAbsenceOfAddressAndEmail() {
        String content = "{\n" +
                "    \"fields_data\": {\n" +
                "        \"first_name\": \"Sunaina\"\n" +
                "    },\n" +
                "    \"field_details\": {\n" +
                "        \"first_name\": {\n" +
                "            \"is_required\": \"true\"\n" +
                "        }\n" +
                "        }\n" +
                "}";
        assertThrows(RuntimeException.class, () -> channels.input().send(MessageBuilder.withPayload(content).build()));
    }

    /**
     * Tests if is_cass_required is false, then even if address is given, address_certification should be 'not_required'
     *
     * @throws InterruptedException    if interrupted while polling the response
     * @throws JsonProcessingException if unable to parse response
     */
    @Test
    void testIsCassNotRequired() throws InterruptedException, JsonProcessingException {
        String input = "{\n" +
                "   \"fields_data\":{\n" +
                "      \"first_name\":\"Test\",\n" +
                "      \"address1\":\"AFF\",\n" +
                "      \"address2\":\"AEE\",\n" +
                "      \"city\":\"HYY\",\n" +
                "      \"state\":\"JJU\",\n" +
                "      \"zip\":\"KKOI\"\n" +
                "   },\n" +
                "   \"field_details\":{\n" +
                "      \"first_name\":{\n" +
                "         \"data_type\":\"TEXT\",\n" +
                "         \"field_type\":\"DEFAULT\",\n" +
                "         \"field_format\":\"PROPERTEXT\",\n" +
                "         \"is_required\":\"true\"\n" +
                "      }\n" +
                "   },\n" +
                "   \"is_cass_required\":false,\n" +
                "   \"email_certification\":\"required\",\n" +
                "   \"phone_certification\":\"required\",\n" +
                "   \"address_certification\":\"required\"\n" +
                "}";
        channels.input().send(MessageBuilder.withPayload(input).build());
        Message<?> response = collector.forChannel(channels.output()).poll(60, TimeUnit.SECONDS);
        JsonNode jsonNode = mapper.readTree((String) Objects.requireNonNull(response).getPayload());
        Map<String, Object> fields = mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
        });
        assertEquals(false, fields.get("is_cass_required"));
        assertEquals(IS_CERTIFICATION_NOT_REQUIRED, fields.get(ADDRESS_CERTIFICATION));
    }

    /**
     * Integration tests for the data validation of the incoming Message
     *
     * @throws InterruptedException    if interrupted while polling the response
     * @throws JsonProcessingException if unable to parse response
     */
    @Test
    void integrationTests() throws JsonProcessingException, InterruptedException {
        String content = "{\n" +
                "\t\"target_table\": \"list_data_ac20060000000002\",\n" +
                "\t\"target_db\": \"lm2_dev\",\n" +
                "\t\"account_id\": \"AC20060000000002\",\n" +
                "\t\"fields_data\": {\n" +
                "\t\t\"first_name\": \"Sunaina\",\n" +
                "\t\t\"email\": \"test @tes.cd \",\n" +
                "\t\t\"address1\": \"AFF\",\n" +
                "\t\t\"address2\": \"AEE\",\n" +
                "\t\t\"city\": \"HYY\",\n" +
                "\t\t\"state\": \"JJU\",\n" +
                "\t\t\"zip\": \"KKOI\",\n" +
                "\t\t\"mobile\": \"\",\n" +
                "\t\t\"recipient_id\": \"RC20070000000001\",\n" +
                "\t\t\"list_id\": \"LI20070000000014\",\n" +
                "\t\t\"locality_code\": \"SL_US\",\n" +
                "\t\t\"job_id\": \"SJ20200000000056\",\n" +
                "\t\t\"contact_source\": \"USER\",\n" +
                "\t\t\"created_by\": \"sluser\",\n" +
                "\t\t\"modified_by\": \"sluser\",\n" +
                "\t\t\"modified_op\": \"I\",\n" +
                "\t\t\"email_status\": \"\",\n" +
                "\t\t\"email_message\": \"\",\n" +
                "\t\t\"sms_status\": \"\",\n" +
                "\t\t\"sms_message\": \"\",\n" +
                "\t\t\"mail_status\": \"\",\n" +
                "\t\t\"mail_message\": \"\"\n" +
                "\t},\n" +
                "\t\"lease_data\": {\n" +
                "\t\t\"target_table\": \"lease_data_ac20060000000002\",\n" +
                "\t\t\"tp_accounts\": [\n" +
                "\t\t\t\"tp1\",\n" +
                "\t\t\t\"tp2\"\n" +
                "\t\t]\n" +
                "\t},\n" +
                "\t\"segment_data\": {\n" +
                "\t\t\"target_table\": \"list_set_data_ac20060000000002\",\n" +
                "\t\t\"segments\": [\n" +
                "\t\t\t\"LI20070000000012\",\n" +
                "\t\t\t\"LI20070000000013\"\n" +
                "\t\t]\n" +
                "\t},\n" +
                "\t\"field_details\": {\n" +
                "\t\t\"first_name\": {\n" +
                "\t\t\t\"data_type\": \"TEXT\",\n" +
                "\t\t\t\"field_type\": \"DEFAULT\",\n" +
                "\t\t\t\"field_format\": \"PROPERTEXT\",\n" +
                "\t\t\t\"is_required\": \"true\"\n" +
                "\t\t},\n" +
                "\t\t\"email\": {\n" +
                "\t\t\t\"data_type\": \"TEXT\",\n" +
                "\t\t\t\"field_type\": \"DEFAULT\",\n" +
                "\t\t\t\"field_format\": \"UNFORMATTED\",\n" +
                "\t\t\t\"is_required\": \"true\"\n" +
                "\t\t}\n" +
                "\t},\n" +
                "\t\"dedupe_fields\": [\n" +
                "\t\t\"first_name\",\n" +
                "\t\t\"email\"\n" +
                "\t],\n" +
                "\t\"country_code\": \"USA\",\n" +
                "\t\"is_cass_required\": true,\n" +
                "\t\"email_certification\": \"required\",\n" +
                "\t\"phone_certification\": \"required\",\n" +
                "\t\"address_certification\": \"required\"\n" +
                "}";
        channels.input().send(MessageBuilder.withPayload(content).build());
        Message<?> response = collector.forChannel(channels.output()).poll(60, TimeUnit.SECONDS);
        JsonNode jsonNode = mapper.readTree((String) Objects.requireNonNull(response).getPayload());
        Map<String, Object> fields = mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
        });
        assertNotNull(fields);
        assertEquals(StringUtils.IS_CERTIFICATION_NOT_REQUIRED, fields.get(StringUtils.PHONE_CERTIFICATION));
    }

    /**
     * Triggers the Spring boot application
     */
    @SpringBootApplication
    public static class DataValidationsApplication {
    }
}
