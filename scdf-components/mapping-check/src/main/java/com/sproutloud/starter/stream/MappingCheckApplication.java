package com.sproutloud.starter.stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sproutloud.starter.stream.dao.impl.MappingCheckDaoImpl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.SendTo;

import java.util.Map;

/**
 * Spring cloud data flow processor using spring boot. 
 * Validated the input message and prepares the mappings and sends the response.
 * 
 * @author mgande
 *
 */
@EnableJdbcRepositories
@EnableBinding(Processor.class)
@SpringBootApplication
public class MappingCheckApplication {

    /**
     * {@link ObjectMapper} to convert Object to required type.
     */
    @Autowired
    private ObjectMapper mapper;

    /**
     * {@link MappingCheck} to validate the input and prepare the mappings.
     */
    @Autowired
    private MappingCheck mappingCheck;

    @Autowired
    private MappingCheckDaoImpl dao;
    /**
     * Triggers the Spring boot application
     *
     * @param args command line args
     */
    public static void main(String[] args) {
        SpringApplication.run(MappingCheckApplication.class, args);
    }

    /**
     * Incoming json is validated against the mandatory check.
     * Checks if integration and integration mappings are present in database with the given integration_id.
     * Reads the csv file from google storage with the location provided and gets headers.
     * Prepares the mappings with integration mappings from db and headers from csv and appends it to input json.
     * Sends the updated input json as output.
     * 
     * @param message Incoming message published to kafka topic.
     * @return {@link Map} after adding the mappings.
     * @throws JsonProcessingException when unable to read input.
     */
    @StreamListener(Processor.INPUT)
    @SendTo(Processor.OUTPUT)
    public Map<String, Object> performAlfMappingCheck(Message<?> message) throws JsonProcessingException {
        JsonNode inputNode = mapper.readTree((String) message.getPayload());
        Map<String, Object> input = mapper.convertValue(inputNode, new TypeReference<Map<String, Object>>() {
        });
        input.put(StringUtils.IN_TIME, System.currentTimeMillis());
        mappingCheck.checkInputAndPrepareMappings(input);
        input.put(StringUtils.JOB_TYPE, "mapping_check");
        Long outTime = System.currentTimeMillis();
        input.put(StringUtils.OUT_TIME, outTime);
        dao.updateJobStatus(input.get("job_id"), outTime);
        return input;
    }
}
