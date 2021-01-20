package com.sproutloud.starter.stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.log4j.Log4j2;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.integration.annotation.Transformer;
import org.springframework.messaging.Message;

import java.util.Map;
import java.util.Objects;

/**
 * 
 * Spring cloud data flow processor using spring boot. Transforms the fields data of input json according to the field details.
 * 
 * @author mgande
 *
 */
@Log4j2
@EnableBinding(Processor.class)
@SpringBootApplication
public class DataTransformationsApplication {

    /**
     * {@link ObjectMapper} for converting incoming message to required type.
     */
    @Autowired
    private ObjectMapper mapper;

    /**
     * {@link DataTransformer} bean for data transformation.
     */
    @Autowired
    private DataTransformer dataTransformer;

    /**
     * Triggers the Spring boot application
     *
     * @param args command line args
     */
    public static void main(String[] args) {
        SpringApplication.run(DataTransformationsApplication.class, args);
    }

    /**
     * Converts the incoming json to readable format and runs transformations on fields data.
     * 
     * @param message Incoming message published to kafka topic
     * @return transformed json details
     * @throws JsonProcessingException when conversion to json is not possible
     */
    @Transformer(inputChannel = Processor.INPUT, outputChannel = Processor.OUTPUT)
    public Map<String, Object> performDataTransformation(Message<?> message) throws JsonProcessingException {
        log.debug("Start of Data Transformation: \n");
        JsonNode inputJsonNode = mapper.readTree((String) message.getPayload());
        Map<String, Object> input = mapper.convertValue(inputJsonNode, new TypeReference<Map<String, Object>>() {
        });
        log.debug("Input details for Data Transformation are: \n " + input);
        input.put(StringUtils.IN_TIME, System.currentTimeMillis());
        if (isInputValid(input)) {
            dataTransformer.transformData(input);
        } else {
            log.error("Input is not valid.");
        }

        prepareOutput(input);
        input.put(StringUtils.JOB_TYPE, "data_transformation");
        input.put(StringUtils.OUT_TIME, System.currentTimeMillis());
        return input;
    }

    /**
     * @param input json from which output is prepared.
     */
    private void prepareOutput(Map<String, Object> input) {
        input.remove("field_details");
        input.remove("country_code");
        input.remove("is_cass_required");
        input.remove("email_certification");
        input.remove("phone_certification");
        input.remove("address_certification");
    }

    /**
     * Validates the input received.
     * 
     * @param input Map<String><Object>
     * @return boolean, true if input is valid
     */
    private boolean isInputValid(Map<String, Object> input) {
        if (Objects.isNull(input) || Objects.isNull(input.get("fields_data"))) {
            return false;
        }
        return true;
    }

}
