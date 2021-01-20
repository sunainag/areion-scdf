package com.sproutloud.starter.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sproutloud.starter.stream.constants.ApplicationConstants;
import com.sproutloud.starter.stream.dao.impl.MappingCheckDaoImpl;
import com.sproutloud.starter.stream.gcp.FileReader;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit Tests for {@link MappingCheck}
 * 
 * @author mgande
 *
 */
@ExtendWith(MockitoExtension.class)
class MappingCheckTest {

    @InjectMocks
    private MappingCheck check;

    @Spy
    private ObjectMapper mapper;

    @Mock
    private MappingCheckDaoImpl dao;

    @Mock
    private FileReader fileReader;

    /**
     * Checks if exception is thrown when input is empty.
     */
    @Test
    void testCheckInputAndPrepareMappingsNullInput() {
        Assertions.assertThrows(RuntimeException.class, () -> check.checkInputAndPrepareMappings(new HashMap<String, Object>()));
    }

    /**
     * Checks if exception is thrown when input is not having all required fields.
     */
    @Test
    void testCheckInputAndPrepareMappingsInvalidInput() {
        Map<String, Object> input = new HashMap<>();
        input.put(ApplicationConstants.INTEGRATION_ID, "1234567");
        Assertions.assertThrows(RuntimeException.class, () -> check.checkInputAndPrepareMappings(input));

        input.put(ApplicationConstants.LIST_ID, "  ");
        Assertions.assertThrows(RuntimeException.class, () -> check.checkInputAndPrepareMappings(input));
    }

    /**
     * Checks if exception is thrown when input has empty dedupe list. 
     */
    @Test
    void testCheckInputAndPrepareMappingsEmptyDedupe() throws JsonProcessingException {
        String input = "{\n" + 
                "    \"integration_id\" : \"FP20080000000335\",  \n" + 
                "    \"list_id\": \"LI20080000000508\",\n" + 
                "    \"account_id\": \"AC20070000000013\",\n" + 
                "    \"target_db\": \"lm2_dev\",\n" + 
                "    \"target_table\": \"list_data_ac20060000000002\",\n" + 
                "    \"job_id\": \"SJ20200000000056\",\n" + 
                "    \"user\": \"sluser\",\n" + 
                "    \"job_type\": \"ingestion\",\n" + 
                "    \"file\": \"gs://sl-dev-listmgmt/integration/test-normalise.csv\",\n" + 
                "    \"contains_headers\": \"true\",\n" + 
                "    \"contact_source\": \"CS20070000000012\",\n" + 
                "    \"lease_table\": \"CS20070000000012\",\n" +
                "    \"segment_table\": \"CS20070000000012\",\n" +
                "    \"dedupe_fields\": [\n" + 
                "    ],\n" + 
                "    \"is_cass_required\": \"true\",\n" + 
                "    \"country_code\": \"USA\"\n" + 
                "}";
        JsonNode node = mapper.readTree(input);
        Assertions.assertThrows(RuntimeException.class, () -> check.checkInputAndPrepareMappings(mapper.convertValue(node, new TypeReference<Map<String, Object>>() {
        })));
    }

    /**
     * Checks if exception is thrown when there are no integration mappings are mapped to integration_id in database.
     */
    @Test
    void testNoAlfMappings() throws JsonProcessingException {
        Map<String, Object> input = prepareInput();
        Map<String, Object> integration = new HashMap<>();
        integration.put("integration_id", "FP20080000000335");
        when(dao.getIntegrations("FP20080000000335", "LI20080000000508", "AC20070000000013")).thenReturn(integration);
        when(dao.getIntegrationMappings("FP20080000000335")).thenReturn(new ArrayList<>());
        Assertions.assertThrows(RuntimeException.class, () -> check.checkInputAndPrepareMappings(input));
    }

    /**
     * Checks if exception is thrown when CSV fiel is not valid.
     */
    @Test
    void testInValidCsvFile() throws JsonProcessingException {
        Map<String, Object> input = prepareInput();
        Map<String, Object> integration = new HashMap<>();
        integration.put("integration_id", "FP20080000000335");
        List<Map<String, Object>> mappings = prepareDbMappings();
        when(dao.getIntegrations("FP20080000000335", "LI20080000000508", "AC20070000000013")).thenReturn(integration);
        when(dao.getIntegrationMappings("FP20080000000335")).thenReturn(mappings);
        when(fileReader.readCsv("gs://sl-dev-listmgmt/integration/test-normalise.csv")).thenReturn(null);
        Assertions.assertThrows(RuntimeException.class, () -> check.checkInputAndPrepareMappings(input));
    }

    /**
     * Checks if exception is thrown when there are headers in CSV file which are not mapped to integration_id in database.
     */
    @Test
    void testInvalidCsvHeader() throws JsonProcessingException {
        Map<String, Object> input = prepareInput();
        Map<String, Object> integration = new HashMap<>();
        integration.put("integration_id", "FP20080000000335");
        List<Map<String, Object>> mappings = prepareDbMappings();
        String[] strings = { "First Name,Last Name,Address 1,tp_id_1,tp_id_2,group_1,group_1,gp_1,test1" };
        InputStream inputStream = new ByteArrayInputStream(
                String.join(System.lineSeparator(), Arrays.asList(strings)).getBytes(StandardCharsets.UTF_8));
        when(dao.getIntegrations("FP20080000000335", "LI20080000000508", "AC20070000000013")).thenReturn(integration);
        when(dao.getIntegrationMappings("FP20080000000335")).thenReturn(mappings);
        when(fileReader.readCsv("gs://sl-dev-listmgmt/integration/test-normalise.csv")).thenReturn(inputStream);
        Assertions.assertThrows(RuntimeException.class, () -> check.checkInputAndPrepareMappings(input));
    }

    /**
     * Checks if exception is thrown when there are dedupe fields which are not in CSV header.
     */
    @Test
    void testInvalidDedupeFields() throws JsonProcessingException {
        Map<String, Object> input = prepareInput();
        Map<String, Object> integration = new HashMap<>();
        integration.put("integration_id", "FP20080000000335");
        List<Map<String, Object>> mappings = prepareDbMappings();
        String[] strings = { "First Name,Last Name,Address 1,tp_id_1,tp_id_2,group_1,group_1,gp_1" };
        InputStream inputStream = new ByteArrayInputStream(
                String.join(System.lineSeparator(), Arrays.asList(strings)).getBytes(StandardCharsets.UTF_8));
        when(dao.getIntegrations("FP20080000000335", "LI20080000000508", "AC20070000000013")).thenReturn(integration);
        when(dao.getIntegrationMappings("FP20080000000335")).thenReturn(mappings);
        when(fileReader.readCsv("gs://sl-dev-listmgmt/integration/test-normalise.csv")).thenReturn(inputStream);
        Assertions.assertThrows(RuntimeException.class, () -> check.checkInputAndPrepareMappings(input));
    }

    /**
     * checks if country code and mappings are updated in input json in success case.
     */
    @Test
    void testNoCountryCode() throws JsonProcessingException {
        Map<String, Object> input = prepareInput();
        input.remove(ApplicationConstants.COUNTRY_CODE);
        Map<String, Object> integration = new HashMap<>();
        integration.put("integration_id", "FP20080000000335");
        List<Map<String, Object>> mappings = prepareDbMappings();
        String[] strings = { "First Name,Last Name,Address 1,tp_id_1,tp_id_2,group_1,group_1,gp_1,Email" };
        InputStream inputStream = new ByteArrayInputStream(
                String.join(System.lineSeparator(), Arrays.asList(strings)).getBytes(StandardCharsets.UTF_8));
        when(dao.getIntegrations("FP20080000000335", "LI20080000000508", "AC20070000000013")).thenReturn(integration);
        when(dao.getIntegrationMappings("FP20080000000335")).thenReturn(mappings);
        when(fileReader.readCsv("gs://sl-dev-listmgmt/integration/test-normalise.csv")).thenReturn(inputStream);
        check.checkInputAndPrepareMappings(input);
        assertEquals("USA", input.get(ApplicationConstants.COUNTRY_CODE));
        assertNotNull(input.get(ApplicationConstants.MAPPINGS));
        assertNull(input.get(ApplicationConstants.INTEGRATION_ID));
        verify(dao).getIntegrationMappings("FP20080000000335");
        verify(dao).getIntegrations("FP20080000000335", "LI20080000000508", "AC20070000000013");
        verify(fileReader).readCsv("gs://sl-dev-listmgmt/integration/test-normalise.csv");
    }

    /**
     * Prepares mock integration mappings that are returned from database call.
     * 
     * @return {@link List} of integration mappings.
     */
    private List<Map<String, Object>> prepareDbMappings() {
        List<Map<String, Object>> mappings = new ArrayList<>();
        mappings.add(prepareMap("FIELD", "First Name", "first_name"));
        mappings.add(prepareMap("FIELD", "Last Name", "last_name"));
        mappings.add(prepareMap("FIELD", "Address 1", "address1"));
        mappings.add(prepareMap("FIELD", "Email", "email"));
        mappings.add(prepareMap("TP_ID", "tp_id_1", ""));
        mappings.add(prepareMap("TP_ID", "tp_id_2", ""));
        mappings.add(prepareMap("GROUP", "group_1", ""));
        mappings.add(prepareMap("GROUP", "group_1", ""));
        mappings.add(prepareMap("GROUP", "gp_1", ""));
        return mappings;
    }

    /**
     * Prepares an integration with given details.
     * 
     * @param mappingType type of mapping
     * @param sourceField    header name in CSV
     * @param targetField    header name to be used furthur.
     * @return {@link Map} of an integration map.
     */
    private Map<String, Object> prepareMap(String mappingType, String sourceField, String targetField) {
        Map<String, Object> map = new HashMap<>();
        map.put("mapping_type", mappingType);
        map.put("source_field", sourceField);
        map.put("target_field", targetField);
        return map;
    }

    /**
     * Prepared input map.
     * 
     * @return {@link Map} of input
     * @throws JsonProcessingException if unable to parse response
     */
    private Map<String, Object> prepareInput() throws JsonProcessingException {
        String input = "{\n" + 
                "    \"integration_id\" : \"FP20080000000335\",  \n" + 
                "    \"list_id\": \"LI20080000000508\",\n" + 
                "    \"account_id\": \"AC20070000000013\",\n" + 
                "    \"target_db\": \"lm2_dev\",\n" + 
                "    \"target_table\": \"list_data_ac20060000000002\",\n" + 
                "    \"job_id\": \"SJ20200000000056\",\n" + 
                "    \"user\": \"sluser\",\n" + 
                "    \"job_type\": \"ingestion\",\n" + 
                "    \"file\": \"gs://sl-dev-listmgmt/integration/test-normalise.csv\",\n" + 
                "    \"contains_headers\": \"true\",\n" + 
                "    \"recipient_source\": \"CS20070000000012\",\n" +
                "    \"lease_table\": \"CS20070000000012\",\n" +
                "    \"segment_table\": \"CS20070000000012\",\n" +
                "    \"dedupe_fields\": [\n" + 
                "        \"first_name\",\n" + 
                "        \"email\"\n" + 
                "    ],\n" + 
                "    \"is_cass_required\": \"true\",\n" + 
                "    \"country_code\": \"USA\"\n" + 
                "}";
        JsonNode node = mapper.readTree(input);
        return mapper.convertValue(node, new TypeReference<Map<String, Object>>() {
        });
    }
}