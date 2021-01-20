package com.sproutloud.starter.stream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sproutloud.starter.stream.dao.LeaseDao;

import lombok.extern.log4j.Log4j2;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.integration.annotation.Transformer;
import org.springframework.messaging.Message;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Application that processes input data to be ingested to lease table and
 * inserts the data into lease table
 * 
 * @author rgupta
 *
 */
@Log4j2
@EnableBinding(Processor.class)
@SpringBootApplication
public class LeaseDataIngestionApplication {

    /**
     * mapper object to be used
     */
    @Autowired
    ObjectMapper mapper;

    /**
     * Object of LeaseDao class to call methods that communicate with database
     */
    @Autowired
    private LeaseDao dao;

    /**
     * Timestamp object to be used for current timestamp
     */
    private Timestamp currentTs;

    /**
     * Triggers the simple spring boot application
     * 
     * @param args
     */
    public static void main(String[] args) {
        SpringApplication.run(LeaseDataIngestionApplication.class, args);
    }

    /**
     * formats the incoming data to be ingested to lease data
     * 
     * @param message
     * @throws IOException
     */
    @Transformer(inputChannel = Processor.INPUT, outputChannel = Processor.OUTPUT)
    public Map<String, Object> formatInputLeaseData(Message<?> message) throws IOException {
        JsonNode jsonNode = mapper.readTree((String) message.getPayload());
        Map<String, Object> leaseInput = mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
        });
        Map<String, Object> aggOutput = new HashMap<>();
        aggOutput.put(com.sproutloud.starter.stream.StringUtils.IN_TIME, System.currentTimeMillis());
        String validityStatus = validityCheck(leaseInput);
        if (validityStatus == "valid") {
            log.debug("********* Processing data to be ingested to lease table: ******************* \n" + leaseInput);
            List<Map<String, Object>> formattedInput = formatData(leaseInput);

            prepareLeaseInsert(formattedInput);
        } else {
            log.error("Input data is invalid: " + validityStatus);
        }

        log.debug("Sending details to data aggregator channel.");
        aggOutput.put(com.sproutloud.starter.stream.StringUtils.JOB_TYPE, "lease_data_ingestion");
        aggOutput.put("job_id", leaseInput.get("job_id"));
        aggOutput.put(com.sproutloud.starter.stream.StringUtils.OUT_TIME, System.currentTimeMillis());
        aggOutput.put("list_id", leaseInput.get("list_id"));
        aggOutput.put("recipient_id", leaseInput.get("recipient_id"));
        return aggOutput;
    }

    /**
     * Checks if all the required fields are present in the input
     * 
     * @param leaseInput
     * @return String
     */
    public String validityCheck(Map<String, Object> leaseInput) {
        List<String> requiredFields = Arrays.asList(new String[] { "target_db", "account_id", "created_by", "modified_by", "modified_op",
                "locality_code", "list_id", "tp_ids", "recipient_id" });
        for (String field : requiredFields) {
            if (StringUtils.isEmpty(leaseInput.get(field))) {
                return "Invalid " + field;
            }
        }
        return "valid";
    }

    /**
     * formats the output data to be sent to sink
     * 
     * @param leaseInput
     * @return List<Map<String,Object>>
     */
    public List<Map<String, Object>> formatData(Map<String, Object> leaseInput) {
        List<String> toAccounts = mapper.convertValue(leaseInput.get("tp_ids"), new TypeReference<List<String>>() {
        });
        List<Map<String, Object>> formattedOutput = new ArrayList<>();

        for (String tpAccount : toAccounts) {
            log.debug("Creating a copy input data to copy incoming fields to output data list");
            Map<String, Object> leaseInputCopy = new HashMap<>();
            Map<String, Object> dbFields = new HashMap<>();
            tpAccount = tpAccount.trim();
            dbFields.put("list_id", leaseInput.get("list_id"));
            dbFields.put("recipient_id", leaseInput.get("recipient_id"));
            dbFields.put("locality_code", leaseInput.get("locality_code"));
            dbFields.put("created_by", leaseInput.get("created_by"));
            dbFields.put("modified_by", leaseInput.get("modified_by"));
            dbFields.put("modified_op", leaseInput.get("modified_op"));
            dbFields.put("account_id", leaseInput.get("account_id"));
            dbFields.put("tp_id", tpAccount);

            leaseInputCopy.put("account_id", leaseInput.get("account_id"));
            leaseInputCopy.put("target_db", leaseInput.get("target_db"));
            leaseInputCopy.put("db_fields", dbFields);

            formattedOutput.add(leaseInputCopy);
        }
        return formattedOutput;
    }

    /**
     * Method to prepare the segment insert statement
     * 
     * @param formattedInput
     */
    public void prepareLeaseInsert(List<Map<String, Object>> formattedInput) {
        currentTs = new Timestamp(System.currentTimeMillis());
        String targetDatabase = (String) formattedInput.get(0).get("target_db");
        String accountId = (String) formattedInput.get(0).get("account_id");
        List<String> columnValues = new ArrayList<>();
        for (Map<String, Object> dbRow : formattedInput) {
            Map<String, String> fieldsMap = new HashMap<>();
            fieldsMap = (Map<String, String>) dbRow.get("db_fields");
            String columnVal = fieldsMap.values().stream().map(val -> "'" + val + "'").collect(Collectors.joining(","));
            columnVal += ",'" + currentTs + "','" + currentTs + "'";
            columnValues.add(columnVal);
        }
        String columnKeys = String.join(",", ((Map<String, String>) formattedInput.get(0).get("db_fields")).keySet());
        columnKeys += ", modified_ts, created_ts";

        dao.runLeaseInsert(targetDatabase, accountId, columnKeys, columnValues);
    }

}
