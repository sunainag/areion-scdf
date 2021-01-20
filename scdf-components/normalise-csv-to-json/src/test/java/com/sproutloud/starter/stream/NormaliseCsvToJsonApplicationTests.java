package com.sproutloud.starter.stream;

import static com.sproutloud.starter.stream.constants.ApplicationConstants.LIST_ID;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.MAPPINGS;
import static org.junit.Assert.assertNotNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sproutloud.starter.stream.dao.impl.FieldsDaoImpl;

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
 * Integration tests for NormaliseCsvToJsonApplication.
 *
 * @author mgande
 *
 */
@SpringBootTest(properties = { "spring.cloud.gcp.credentials.location=${GCP_CREDENTIALS_LOCATION}",
        "spring.datasource.url=jdbc:postgresql://localhost:26257/lm?prepareThreshold=0", "spring.datasource.username=sluser",
        "spring.datasource.password=sproutloud", "spring.datasource.platform=postgresql" })
class NormaliseCsvToJsonApplicationTests {

    @Autowired
    protected Processor processor;

    @Autowired
    protected MessageCollector collector;

    @Autowired
    protected ObjectMapper mapper;

    @Autowired
    CsvToJsonNormalize normalizer;

    @Autowired
    FieldsDaoImpl dao;

    @Test
    void testPrepareFields() throws JsonProcessingException {
        Map<String, Object> input = new HashMap<>();
        String list_id = "LI20090000000004";
        input.put(LIST_ID, list_id);

        String in = "{\n" + "  \"job_type\": \"alf_ingestion\",\n" + "  \"file\": \"gs://sl-dev-listmgmt/alf/test-normalise.csv\",\n"
                + "  \"contains_headers\": \"true\",\n" + "  \"mappings\": {\n" + "    \"first_name\": \"1\",\n" + "    \"middle_name\": \"2\",\n"
                + "    \"last_name\": \"3\",\n" + "    \"address1\": \"4\",\n" + "    \"address2\": \"5\",\n" + "    \"city\": \"6\",\n"
                + "    \"state\": \"7\",\n" + "    \"zip\": \"8\",\n" + "    \"country\": \"9\",\n" + "    \"company\": \"10\",\n"
                + "    \"email\": \"11\",\n" + "    \"govt_id\": \"12\",\n" + "    \"tp_id\": \"13\",\n" + "    \"group_1\": \"14\",\n"
                + "    \"group_2\": \"15\"\n" + "  },\n" + "  \"account_id\": \"AC20060000000002\",\n" + "  \"list_id\": \"LI20090000000004\",\n"
                + "  \"target_table\": \"list_data_ac20070000000013\",\n" + "  \"dedupe_fields\": [\n" + "    \"first_name\",\n" + "    \"email\"\n"
                + "  ],\n" + "  \"is_cass_required\": \"false\",\n" + "  \"recipient_source\": \"CS20200000000001\",\n"
                + "  \"target_db\": \"lm2_dev\",\n" + "  \"country_code\": \"USA\",\n" + "  \"user\": \"sluser\",\n"
                + "  \"job_id\": \"SJ20080000000930\"\n" + "}";
        JsonNode inputNode = mapper.readTree((String) in);
        Map<String, Object> fields = mapper.convertValue(inputNode, new TypeReference<Map<String, Object>>() {
        });
        Map<String, Integer> fieldIndexMap = mapper.convertValue(fields.get(MAPPINGS), new TypeReference<Map<String, Integer>>() {
        });

        dao.getListFieldsData(list_id, fieldIndexMap.keySet());
        normalizer.getFieldDetails(input, fieldIndexMap);
    }

    @Test
    void testNormalizeCsvToJsonSuccess() throws InterruptedException, JsonProcessingException {
        String input = "{\n" + 
                "   \"job_type\":\"alf_ingestion\",\n" + 
                "   \"file\":\"gs://sl-dev-listmgmt/alf/test-pipeline-3.csv\",\n" + 
                "   \"contains_headers\":\"true\",\n" + 
                "   \"mappings\":{\n" + 
                "     \"zip\":8,\n" + 
                "     \"country\":9,\n" + 
                "     \"address2\":5,\n" + 
                "     \"city\":6,\n" + 
                "     \"address1\":4,\n" + 
                "     \"mobile\":16,\n" + 
                "     \"last_name\":3,\n" + 
                "     \"middle_name\":2,\n" + 
                "     \"group_1\":14,\n" + 
                "     \"tp_id_1\":13,\n" + 
                "     \"group_2\":15,\n" + 
                "     \"govt_id\":12,\n" + 
                "     \"company\":10,\n" + 
                "     \"state\":7,\n" + 
                "     \"first_name\":1,\n" + 
                "     \"email\":11\n" + 
                "   },\n" + 
                "   \"account_id\":\"AC20060000000002\",\n" + 
                "   \"list_id\":\"LI20090000000004\",\n" + 
                "   \"target_table\":\"list_data_ac20070000000013\",\n" + 
                "   \"dedupe_fields\":[\n" + 
                "      \"first_name\",\n" + 
                "      \"email\"\n" + 
                "   ],\n" + 
                "   \"is_cass_required\":\"true\",\n" + 
                "   \"recipient_source\":\"CS20200000000001\",\n" + 
                "   \"target_db\":\"lm2_dev\",\n" + 
                "   \"country_code\":\"USA\",\n" + 
                "   \"user\":\"sluser\",\n" + 
                "   \"lease_table\":\"lease_data_ac20070000000013\",\n" + 
                "   \"segment_table\":\"list_set_data_ac20070000000013\",\n" + 
                "   \"job_id\":\"SJ20080000000891\"\n" + 
                "}";
        processor.input().send(MessageBuilder.withPayload(input).build());
        Message<?> response = collector.forChannel(processor.output()).poll(60, TimeUnit.SECONDS);
        JsonNode jsonNode = mapper.readTree((String) Objects.requireNonNull(response).getPayload());
        Map<String, Object> fields = mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
        });
        assertNotNull(fields);
    }

}
