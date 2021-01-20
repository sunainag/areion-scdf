package com.sproutloud.starter.stream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.integration.annotation.Transformer;
import org.springframework.messaging.Message;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Processor class to initiate the de-duplication process for the input data
 *
 * @author rgupta
 */
@Slf4j
@EnableBinding(Processor.class)
@EnableJdbcRepositories
@SpringBootApplication
public class DeduplicationApplication {

    @Autowired
    ObjectMapper mapper;
    @Autowired
    private DedupeHandler dedupeHandler;

    /**
     * Triggers spring boot application
     *
     * @param args
     */
    public static void main(String[] args) {
        SpringApplication.run(DeduplicationApplication.class, args);
    }

    /**
     * Accepts the input from the input channel and processes it for
     * dedupe
     *
     * @param message
     * @return the output JSON with router_flag and existing db values
     * @throws IOException
     */
    @Transformer(inputChannel = Processor.INPUT, outputChannel = Processor.OUTPUT)
    public Map<String, Object> dedupeProcessor(Message<?> message) throws IOException {
        JsonNode jsonNode = mapper.readTree((String) message.getPayload());
        Map<String, Object> dedupeInput = mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
        });
        dedupeInput.put(com.sproutloud.starter.stream.StringUtils.IN_TIME, System.currentTimeMillis());
        String validityFailureMsg = "Invalid input passed";
        if (!StringUtils.isEmpty(dedupeInput)) {
            Map<String, Object> fieldData = mapper.convertValue(dedupeInput.get("fields_data"),
                    new TypeReference<Map<String, Object>>() {
                    });

            String db = (String) dedupeInput.get("target_db");
            String table = (String) dedupeInput.get("target_table");
            validityFailureMsg = dedupeHandler.isValid(fieldData, db, table);
            if (Objects.equals(validityFailureMsg, "valid")) {
                log.debug("Processing Dedupe:" + dedupeInput);
                Map<String, String> existingEntry = dedupeHandler.execute(fieldData, db, table);
                log.debug("Setting the router_flag to INSERT or UPDATE");
                if (CollectionUtils.isEmpty(existingEntry)) {
                    dedupeInput.put("router_flag", "INSERT");
                } else {
                    dedupeInput.put("router_flag", "UPDATE");
                    dedupeInput.put("db_fields", existingEntry);
                }
                dedupeInput.put(com.sproutloud.starter.stream.StringUtils.JOB_TYPE, "dedupe");
                dedupeInput.put(com.sproutloud.starter.stream.StringUtils.OUT_TIME, System.currentTimeMillis());
                return dedupeInput;
            }
        }
        log.error(validityFailureMsg);
        throw new RuntimeException(validityFailureMsg);
    }
}
