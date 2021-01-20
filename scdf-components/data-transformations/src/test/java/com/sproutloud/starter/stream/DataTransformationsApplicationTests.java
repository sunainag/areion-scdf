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
 * Runs the integration tests for Data Transformation Application.
 * 
 * @author mgande
 *
 */
@SpringBootTest
class DataTransformationsApplicationTests {

    @Autowired
    protected Processor processor;

    @Autowired
    protected MessageCollector collector;

    @Autowired
    protected ObjectMapper mapper;

    /**
     * Integration tests for the data transformation of the incoming Message
     * 
     * @throws InterruptedException    if interrupted while polling the response
     * @throws JsonProcessingException if unable to parse response
     */
    @Test
    void testPerformDataTransformation() throws InterruptedException, JsonProcessingException {
        String input = "{\n" + 
                "   \"target_table\":\"list_data_ac20060000000002\",\n" + 
                "   \"target_db\":\"lm2_dev\",\n" + 
                "   \"account_id\":\"AC20060000000002\",\n" + 
                "   \"fields_data\":{\n" + 
                "      \"first_name\":\"Naveen\",\n" + 
                "      \"email\":\"nave@tes.cd\",\n" + 
                "      \"address1\":\"AFF\",\n" + 
                "      \"address2\":\"AEE\",\n" + 
                "      \"city\":\"HYY\",\n" + 
                "      \"state\":\"JJU\",\n" + 
                "      \"zip\":\"\",\n" + 
                "      \"phone\":\"9899\",\n" +
                "      \"mobile\":\"text\",\n" +
                "      \"recipient_id\":\"RC20070000000001\",\n" + 
                "      \"list_id\":\"LI20070000000014\",\n" + 
                "      \"locality_code\":\"SL_US\",\n" + 
                "      \"job_id\":\"SJ20200000000056\",\n" + 
                "      \"contact_source\":\"USER\",\n" + 
                "      \"created_by\":\"sluser\",\n" + 
                "      \"modified_by\":\"sluser\",\n" + 
                "      \"modified_op\":\"I\",\n" + 
                "      \"date\":\"12/12/1995\",\n" + 
                "      \"date1\":\"12121995\",\n" + 
                "      \"boolean1\":\"true\",\n" + 
                "      \"boolean2\":\"NULL\",\n" + 
                "      \"boolean3\":\"\",\n" + 
                "      \"boolean4\":\"false\",\n" + 
                "      \"boolean5\":\"NULL\",\n" + 
                "      \"boolean6\":\"\"\n" + 
                "   },\n" + 
                "   \"lease_data\":{\n" + 
                "      \"target_table\":\"lease_data_ac20060000000002\",\n" + 
                "      \"tp_accounts\":[\n" + 
                "         \"tp1\",\n" + 
                "         \"tp2\"\n" + 
                "      ]\n" + 
                "   },\n" + 
                "   \"segment_data\":{\n" + 
                "      \"target_table\":\"list_set_data_ac20060000000002\",\n" + 
                "      \"segments\":[\n" + 
                "         \"LI20070000000012\",\n" + 
                "         \"LI20070000000013\"\n" + 
                "      ]\n" + 
                "   },\n" + 
                "   \"field_details\":{\n" + 
                "      \"first_name\":{\n" + 
                "         \"data_type\":\"TEXT\",\n" + 
                "         \"field_type\":\"DEFAULT\",\n" + 
                "         \"field_format\":\"LOWERCASE\",\n" + 
                "         \"is_required\":\"true\"\n" + 
                "      },\n" + 
                "      \"created_by\":{\n" + 
                "         \"data_type\":\"TEXT\",\n" + 
                "         \"field_type\":\"DEFAULT\",\n" + 
                "         \"field_format\":\"PROPERCASE\",\n" + 
                "         \"is_required\":\"true\"\n" + 
                "      },\n" +
                "      \"zip\":{\n" + 
                "         \"data_type\":\"TEXT\",\n" + 
                "         \"field_type\":\"DEFAULT\",\n" + 
                "         \"field_format\":\"PROPERCASE\",\n" + 
                "         \"is_required\":\"true\"\n" + 
                "      },\n" +
                "      \"state\":{\n" + 
                "         \"data_type\":\"TEXT\",\n" + 
                "         \"field_type\":\"DEFAULT\",\n" + 
                "         \"field_format\":\"\",\n" + 
                "         \"is_required\":\"true\"\n" + 
                "      },\n" +
                "      \"city\":{\n" + 
                "         \"data_type\":\"\",\n" + 
                "         \"field_type\":\"DEFAULT\",\n" + 
                "         \"field_format\":\"\",\n" + 
                "         \"is_required\":\"true\"\n" + 
                "      },\n" +
                "      \"modified_by\":{\n" + 
                "         \"data_type\":\"TEXT\",\n" + 
                "         \"field_type\":\"DEFAULT\",\n" + 
                "         \"field_format\":\"UPPERCASE\",\n" + 
                "         \"is_required\":\"true\"\n" + 
                "      },\n" + 
                "      \"phone\":{\n" + 
                "         \"data_type\":\"NUMERIC\",\n" + 
                "         \"field_type\":\"DEFAULT\",\n" + 
                "         \"field_format\":\"\",\n" + 
                "         \"is_required\":\"true\"\n" + 
                "      },\n" +
                "      \"mobile\":{\n" + 
                "         \"data_type\":\"NUMERIC\",\n" + 
                "         \"field_type\":\"DEFAULT\",\n" + 
                "         \"field_format\":\"\",\n" + 
                "         \"is_required\":\"true\"\n" + 
                "      },\n" +
                "      \"date\":{\n" + 
                "         \"data_type\":\"DATE\",\n" + 
                "         \"field_type\":\"DEFAULT\",\n" + 
                "         \"field_format\":\"\",\n" + 
                "         \"is_required\":\"true\"\n" + 
                "      },\n" +
                "      \"date1\":{\n" + 
                "         \"data_type\":\"DATE\",\n" + 
                "         \"field_type\":\"DEFAULT\",\n" + 
                "         \"field_format\":\"\",\n" + 
                "         \"is_required\":\"true\"\n" + 
                "      },\n" +
                "      \"email\":{\n" + 
                "         \"data_type\":\"TEXT\",\n" + 
                "         \"field_type\":\"SYSTEM\",\n" + 
                "         \"field_format\":\"UNFORMATTED\",\n" + 
                "         \"is_required\":\"true\"\n" + 
                "      },\n" + 
                "      \"boolean1\":{\n" + 
                "         \"data_type\":\"BOOLEAN\",\n" + 
                "         \"field_type\":\"DEFAULT\",\n" + 
                "         \"field_format\":\"\",\n" + 
                "         \"is_required\":\"true\"\n" + 
                "      },\n" + 
                "      \"boolean2\":{\n" + 
                "         \"data_type\":\"BOOLEAN\",\n" + 
                "         \"field_type\":\"DEFAULT\",\n" + 
                "         \"field_format\":\"\",\n" + 
                "         \"is_required\":\"true\"\n" + 
                "      },\n" + 
                "      \"boolean3\":{\n" + 
                "         \"data_type\":\"BOOLEAN\",\n" + 
                "         \"field_type\":\"SYSTEM\",\n" + 
                "         \"field_format\":\"\",\n" + 
                "         \"is_required\":\"true\"\n" + 
                "      },\n" + 
                "      \"boolean4\":{\n" + 
                "         \"data_type\":\"BOOLEAN\",\n" + 
                "         \"field_type\":\"DEFAULT\",\n" + 
                "         \"field_format\":\"\",\n" + 
                "         \"is_required\":\"true\"\n" + 
                "      },\n" + 
                "      \"boolean5\":{\n" + 
                "         \"data_type\":\"BOOLEAN\",\n" + 
                "         \"field_type\":\"DEFAULT\",\n" + 
                "         \"field_format\":\"\",\n" + 
                "         \"is_required\":\"true\"\n" + 
                "      },\n" + 
                "      \"boolean6\":{\n" + 
                "         \"data_type\":\"BOOLEAN\",\n" + 
                "         \"field_type\":\"\",\n" + 
                "         \"field_format\":\"\",\n" + 
                "         \"is_required\":\"true\"\n" + 
                "      }\n" + 
                "   },\n" + 
                "   \"dedupe_fields\":[\n" + 
                "      \"first_name\",\n" + 
                "      \"email\"\n" + 
                "   ],\n" + 
                "   \"country_code\":\"USA\",\n" + 
                "   \"is_cass_required\":true,\n" + 
                "   \"email_certification\":\"done\",\n" + 
                "   \"phone_certification\":\"required\",\n" + 
                "   \"address_certification\":\"required\"\n" + 
                "}";
        processor.input().send(MessageBuilder.withPayload(input).build());
        Message<?> response = collector.forChannel(processor.output()).poll(60, TimeUnit.SECONDS);
        JsonNode jsonNode = mapper.readTree((String) Objects.requireNonNull(response).getPayload());
        Map<String, Object> output = mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
        });
        Map<String, Object> fieldDetails = mapper.convertValue(output.get("fields_data"), new TypeReference<Map<String, Object>>() {
        });
        assertNotNull(output);
        assertNull(output.get("field_details"));
        assertEquals(fieldDetails.get("first_name"), "naveen");
        assertEquals(fieldDetails.get("created_by"), "Sluser");
        assertEquals(fieldDetails.get("modified_by"), "SLUSER");
        assertEquals(fieldDetails.get("email"), "nave@tes.cd");
        assertEquals(fieldDetails.get("boolean3"), "false");
        
    }

}
