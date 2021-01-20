package com.sproutloud.starter.stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.messaging.Message;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Runs unit and integration tests
 * Tests the incoming message format and missing fields
 *
 * @author sunainag
 */
@SpringBootTest
class CertificationRouterApplicationTests {

    final String allCertifications = "{\n" +
            "    \"email_certification\": \"required\",\n" +
            "    \"phone_certification\": \"required\",\n" +
            "    \"address_certification\": \"required\"\n" +
            "}";
    final String twoCertifications = "{\n" +
            "    \"email_certification\": \"not_required\",\n" +
            "    \"phone_certification\": \"required\",\n" +
            "    \"address_certification\": \"required\"\n" +
            "}";
    final String noCertifications = "{\n" +
            "    \"email_certification\": \"not_required\",\n" +
            "    \"phone_certification\": \"not_required\",\n" +
            "    \"address_certification\": \"not_required\"\n" +
            "}";
    final String missingCertifications = "{}";
    @Autowired
    ObjectMapper mapper;
    @Autowired
    MultiOutputProcessor processor;
    @SpyBean
    CertificationRouterApplication routerApplication;
    @Autowired
    MessageCollector collector;

    @Test
    void contextLoads() {
        assertNotNull(processor);
    }

    @ParameterizedTest
    @ValueSource(strings = {allCertifications, twoCertifications, noCertifications, missingCertifications})
    void testMethod_Route(String json) throws JsonProcessingException {
        SubscribableChannel routerInput = processor.routerInput();
        routerApplication.route(new GenericMessage<>(json));
        BlockingQueue<Message<?>> messages = collector.forChannel(processor.emailOutput());
        assertNotNull(messages.element());
    }

    @Test
    void testRouter() throws JsonProcessingException {
        SubscribableChannel routerInput = processor.routerInput();
        String json = "{\"fields_data\": {},\"field_details\": {}}";
        routerApplication.route(new GenericMessage<>(json));
        BlockingQueue<Message<?>> messages = collector.forChannel(processor.transformationOutput());
        assertNotNull(messages.element());
    }

    @Test
    void integrationTests() throws InterruptedException, JsonProcessingException {
        String content = "{\n" +
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
                "        \"phone\": \"9899\",\n" +
                "        \"recipient_id\": \"RC20070000000001\",\n" +
                "        \"list_id\": \"LI20070000000014\",\n" +
                "        \"locality_code\": \"SL_US\",\n" +
                "        \"job_id\": \"SJ20200000000056\",\n" +
                "        \"contact_source\": \"USER\",\n" +
                "        \"created_by\": \"sluser\",\n" +
                "        \"modified_by\": \"sluser\",\n" +
                "        \"modified_op\": \"I\",\n" +
                "        \"email_status\": \"\",\n" +
                "        \"email_message\": \"\",\n" +
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
                "        },\n" +
                "       \"other_fields\": {}\n" +
                "    },\n" +
                "    \"dedupe_fields\": [\n" +
                "        \"first_name\",\n" +
                "        \"email\"\n" +
                "    ],\n" +
                "    \"country_code\": \"USA\",\n" +
                "    \"is_cass_required\": true,\n" +
                "    \"email_certification\": \"required\",\n" +
                "    \"phone_certification\": \"required\",\n" +
                "    \"address_certification\": \"required\"\n" +
                "}";
        processor.routerInput().send(MessageBuilder.withPayload(content).build());
        Message<?> response = collector.forChannel(processor.emailOutput()).poll(60, TimeUnit.SECONDS);
        JsonNode responseNode = mapper.readTree((String) Objects.requireNonNull(response).getPayload());
        Map<String, Object> fields = mapper.convertValue(responseNode, new TypeReference<Map<String, Object>>() {
        });
        assertNotNull(fields);
        assertEquals(StringUtils.IS_CERTIFICATION_REQUIRED, fields.get(StringUtils.EMAIL_CERTIFICATION));
    }
}
