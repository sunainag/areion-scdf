package com.sproutloud.starter.stream;

import static com.sproutloud.starter.stream.constants.ApplicationConstants.ACCOUNT_ID;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.MAPPING_TYPE;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.INTEGRATION_ID;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.COUNTRY_CODE;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.DEDUPE_FIELDS;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.FILE;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.GROUP;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.LIST_ID;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.MAPPINGS;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.SOURCE_FIELD;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.TARGET_FIELD;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.TP_ID;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.Storage;
import com.opencsv.CSVReader;
import com.sproutloud.starter.stream.dao.impl.MappingCheckDaoImpl;
import com.sproutloud.starter.stream.gcp.FileReader;

import lombok.extern.log4j.Log4j2;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Validated the incoming json and prepares the fields mappings.
 * 
 * @author mgande
 *
 */
@Log4j2
@Component
public class MappingCheck {

    /**
     * {@link ObjectMapper} to convert Object to required type.
     */
    @Autowired
    private ObjectMapper mapper;

    /**
     * {@link MappingCheckDaoImpl} for database interaction.
     */
    @Autowired
    private MappingCheckDaoImpl dao;

    /**
     * {@link Storage} for reading csv from google storage.
     */
    @Autowired
    private FileReader fileReader;

    /**
     * Incoming json is validated against the mandatory check. Checks if integrations and integration mappings are present in database with the given
     * integration_id. Reads the csv file from google storage with the location provided and gets headers. Prepares the mappings with integration mappings
     * from db and headers from csv and appends it to input json.
     * 
     * @param input incoming json.
     */
    public void checkInputAndPrepareMappings(Map<String, Object> input) {
        boolean isValid = validateInput(input);
        if (isValid) {
            log.debug("Checking if integrations with given integration_id, list_id and account_id is present in the database.");
            dao.getIntegrations((String) input.get(INTEGRATION_ID), (String) input.get(LIST_ID), (String) input.get(ACCOUNT_ID));

            log.debug("Getting integration_mappings from database.");
            List<Map<String, Object>> integrationMappings = dao.getIntegrationMappings((String) input.get(INTEGRATION_ID));
            if (CollectionUtils.isEmpty(integrationMappings)) {
                throw new RuntimeException("Alf mappings are not present for the given integration_id.");
            }
            Map<String, String> sourceAndTargetFieldAndTypeMap = new HashMap<>();
            integrationMappings.stream().forEach(map -> {
                String targetFieldAndType = (String) map.get(TARGET_FIELD) + "::" + (String) map.get(MAPPING_TYPE);
                sourceAndTargetFieldAndTypeMap.put((String) map.get(SOURCE_FIELD), targetFieldAndType);
            });
            String[] headers = getHeadersFromCsv((String) input.get(FILE));
            log.debug("Preparing mappings from the database details and csv header details.");
            Map<String, Integer> fieldIndexMap = prepareFieldIndexMap(sourceAndTargetFieldAndTypeMap, headers);
            List<String> dedupeFields = mapper.convertValue(input.get(DEDUPE_FIELDS), new TypeReference<List<String>>() {
            });

            log.debug("Checking if the dedupe_fields are present in the mappings.");
            if (!fieldIndexMap.keySet().containsAll(dedupeFields)) {
                throw new RuntimeException("Dedupe Fields are not present in mappings.");
            }
            input.put(MAPPINGS, fieldIndexMap);
            input.remove(INTEGRATION_ID);
            if (!input.containsKey(COUNTRY_CODE) || StringUtils.isEmpty(input.get(COUNTRY_CODE))) {
                input.put(COUNTRY_CODE, "USA");
            }
        } else {
            log.error("Input message is not having all the required fields.");
            throw new RuntimeException("Input is not Valid.");
        }

    }

    /**
     * Prepared the field index map with the sourceAndTargetFieldAndTypeMap and headers from csv.
     * 
     * @param sourceAndTargetFieldAndTypeMap {@link Map} with key as source_field and values as target_field::mapping_type.
     * @param headers                        of the csv file.
     * @return {@link Map} with key as field name and value as {@link Integer} column name in csv.
     */
    private Map<String, Integer> prepareFieldIndexMap(Map<String, String> sourceAndTargetFieldAndTypeMap, String[] headers) {
        Map<String, Integer> fieldIndexMap = new HashMap<>();
        Integer groupCounter = 1;
        Integer accountCounter = 1;
        log.debug("Checking if the values in the header of the csv are present in source_fields of integration_mappings.");
        for (int i = 0; i < headers.length; i++) {
            if (!sourceAndTargetFieldAndTypeMap.containsKey(headers[i])) {
                log.error("CSV header: " + headers[i] + "is not present in integration mappings.");
                throw new RuntimeException("Csv header is not present in integration mappings.");
            }
            String[] split = sourceAndTargetFieldAndTypeMap.get(headers[i]).split("::");
            String targetField = split[0];
            String fieldType = split[1];
            if (TP_ID.equals(fieldType)) {
                targetField = "tp_id_" + accountCounter;
                accountCounter++;
            } else if (GROUP.equals(fieldType)) {
                targetField = "group_" + groupCounter;
                groupCounter++;
            }
            if (StringUtils.isEmpty(targetField)) {
                throw new RuntimeException("Header name is not proper.");
            }
            fieldIndexMap.put(targetField, i + 1);
        }
        return fieldIndexMap;
    }

    /**
     * Reads the csv file from google storage.
     * 
     * @param file location of csv file in google storage.
     * @return String array with headers.
     */
    private String[] getHeadersFromCsv(String file) {
        InputStream in = fileReader.readCsv(file);
        try (CSVReader reader = new CSVReader(new BufferedReader(new InputStreamReader(in)));){
            return reader.readNext();
        } catch (Exception e) {
            log.error("Unable to Read header from InputStream.");
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * Validated the input for mandatory checks.
     * 
     * @param input incoming json request.
     * @return true, if valid.
     */
    private boolean validateInput(Map<String, Object> input) {
        log.debug("Checking if mandatory fields are passed in the input.");
        if (CollectionUtils.isEmpty(input)) {
            return false;
        }
        List<String> requiredFields = Arrays.asList(new String[] { INTEGRATION_ID, LIST_ID, ACCOUNT_ID, "target_db", "target_table", "job_id", "user",
                "job_type", FILE, "contains_headers", "recipient_source", DEDUPE_FIELDS, "is_cass_required", "segment_table", "lease_table" });
        for (String field : requiredFields) {
            if (!input.containsKey(field) || StringUtils.isEmpty(input.get(field))) {
                return false;
            }
        }
        List<String> dedupeFields = mapper.convertValue(input.get(DEDUPE_FIELDS), new TypeReference<List<String>>() {
        });
        if (CollectionUtils.isEmpty(dedupeFields)) {
            return false;
        }
        return true;

    }
}
