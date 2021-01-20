package com.sproutloud.starter.stream;

import static com.sproutloud.starter.stream.StringUtils.ADDRESS_CERTIFICATION;
import static com.sproutloud.starter.stream.StringUtils.EMAIL_CERTIFICATION;
import static com.sproutloud.starter.stream.StringUtils.IS_CASS_REQUIRED;
import static com.sproutloud.starter.stream.StringUtils.IS_CERTIFICATION_NOT_REQUIRED;
import static com.sproutloud.starter.stream.StringUtils.IS_CERTIFICATION_REQUIRED;
import static com.sproutloud.starter.stream.StringUtils.PHONE_CERTIFICATION;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sproutloud.starter.stream.exception.FieldValidationException;
import com.sproutloud.starter.stream.exception.InsufficientDataException;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.integration.annotation.Transformer;
import org.springframework.messaging.Message;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.Map;
import java.util.Objects;

/**
 * Spring cloud data flow processor using spring boot.
 * Validates the input json for the given mandatory fields.
 *
 * @author sgoyal
 */
@Slf4j
@EnableBinding(Processor.class)
@SpringBootApplication
public class DataValidationsApplication {

    @Autowired
    ObjectMapper mapper;

    /**
     * Triggers the Spring boot application
     *
     * @param args command line args
     */
    public static void main(String[] args) {
        SpringApplication.run(DataValidationsApplication.class, args);
    }

    /**
     * Converts the incoming json to readable format and runs the validations on the input data.
     *
     * @param message Incoming message published to kafka topic
     * @return the validated json details
     */
    @Transformer(inputChannel = Processor.INPUT, outputChannel = Processor.OUTPUT)
    public Map<String, Object> validate(Message<?> message) throws JsonProcessingException {
        log.info("Processing event: \n"+message.getPayload());
        JsonNode jsonNode = mapper.readTree((String) message.getPayload());
        Map<String, Object> input = mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
        });
        input.put(com.sproutloud.starter.stream.StringUtils.IN_TIME, System.currentTimeMillis());
        return validate(input);
    }

    /**
     * Validates the incoming json for mandatory fields using field details and corresponding data. Sets the
     * certifications is required any.
     *
     * @param input the incoming json
     * @return Map<String, Object> validated output
     */
    private Map<String, Object> validate(Map<String, Object> input) {
        log.info("Validating: \n"+input);
        if (Objects.isNull(input.get("field_details")) || Objects.isNull(input.get("fields_data"))) {
            input.put(com.sproutloud.starter.stream.StringUtils.JOB_TYPE, "validation_failed");
            input.put(com.sproutloud.starter.stream.StringUtils.OUT_TIME, System.currentTimeMillis());
            throw new InsufficientDataException("Fields data or field details are missing in incoming message");
        }
        Object fieldDetailsJson = input.get("field_details");
        Object fieldDataJson = input.get("fields_data");
        Map<String, Map<String, String>> fieldDetails = mapper.convertValue(fieldDetailsJson,
                new TypeReference<Map<String, Map<String, String>>>() {
                });
        Map<String, Object> fieldData = mapper.convertValue(fieldDataJson, new TypeReference<Map<String, Object>>() {
        });
        if (CollectionUtils.isEmpty(fieldData) || CollectionUtils.isEmpty(fieldDetails)) {
            input.put(com.sproutloud.starter.stream.StringUtils.JOB_TYPE, "validation_failed");
            input.put(com.sproutloud.starter.stream.StringUtils.OUT_TIME, System.currentTimeMillis());
            throw new InsufficientDataException("Fields data or field details are empty");
        }
        log.debug("Validating required fields' data should not be missing");
        for (Map.Entry<String, Map<String, String>> fieldDetail : Objects.requireNonNull(fieldDetails).entrySet()) {
            String fieldName = fieldDetail.getKey();
            Map<String, String> fieldCategories = fieldDetail.getValue();
            if (Objects.equals(fieldCategories.get("is_required"), "t") && StringUtils.isEmpty(fieldData.get(fieldName))) {
                input.put(com.sproutloud.starter.stream.StringUtils.JOB_TYPE, "validation_failed");
                input.put(com.sproutloud.starter.stream.StringUtils.OUT_TIME, System.currentTimeMillis());
                throw new FieldValidationException("Required/mandatory fields cannot be empty for record");
            }
        }
        log.debug("Validating the combination of first_name with email, or first_name with address details (address1," +
                " address2, city, state and zip)");
        if (StringUtils.isEmpty(fieldData.get("first_name")) || (StringUtils.isEmpty(fieldData.get("email")) && hasMissingAddress(fieldData))) {
            throw new FieldValidationException("Combination of either of (first_name,email) or (first_name,address) " +
                    "is mandatory, both are missing");
        }
        appendCertifications(fieldData, input);
        log.info("Validated for "+input.get("list_id"));
        input.put(com.sproutloud.starter.stream.StringUtils.JOB_TYPE, "validation_success");
        input.put(com.sproutloud.starter.stream.StringUtils.OUT_TIME, System.currentTimeMillis());
        return input;
    }

    /**
     * @param fieldData Data for each fieldName passed in the incoming json
     * @return if contains address field values, return true
     */
    private boolean hasMissingAddress(Map<String, Object> fieldData) {
        return StringUtils.isEmpty(fieldData.get("address1")) || StringUtils.isEmpty(fieldData.get("address2")) || StringUtils.isEmpty(fieldData.get("state")) || StringUtils.isEmpty(fieldData.get("city")) || StringUtils.isEmpty(fieldData.get("zip"));
    }

    /**
     * Sets whether given certifications are required or not
     *
     * @param fieldData Data for each fieldName passed in the incoming json
     * @param input     the incoming json
     */
    private void appendCertifications(Map<String, Object> fieldData, Map<String, Object> input) {
        input.put(EMAIL_CERTIFICATION, IS_CERTIFICATION_NOT_REQUIRED);
        input.put(PHONE_CERTIFICATION, IS_CERTIFICATION_NOT_REQUIRED);
        input.put(ADDRESS_CERTIFICATION, IS_CERTIFICATION_NOT_REQUIRED);

        if (!StringUtils.isEmpty(fieldData.get("email"))) {
            input.put(EMAIL_CERTIFICATION, IS_CERTIFICATION_REQUIRED);
        }
        if (!StringUtils.isEmpty(fieldData.get("mobile"))) {
            input.put(PHONE_CERTIFICATION, IS_CERTIFICATION_REQUIRED);
        }
        if (Objects.equals(true, input.get(IS_CASS_REQUIRED)) && !StringUtils.isEmpty(fieldData.get("address1"))) {
            input.put(ADDRESS_CERTIFICATION, IS_CERTIFICATION_REQUIRED);
        }
    }
}
