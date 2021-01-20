package com.sproutloud.starter.stream;

import static com.sproutloud.starter.stream.constants.ApplicationConstants.EMAIL;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.FIELDS_DATA;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.VALID_STATUS;

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
 * Spring cloud data flow processor using spring boot. Performs Email certification and sets status accordingly.
 * 
 * @author mgande
 *
 */
@Log4j2
@EnableBinding(Processor.class)
@SpringBootApplication
public class EmailCertificationApplication {

    /**
     * {@link EmailCertification} Bean for certifying email.
     */
    @Autowired
    private EmailCertification emailCertification;

    /**
     * {@link ObjectMapper} for converting incoming message to required type.
     */
    @Autowired
    private ObjectMapper mapper;

    /**
     * Triggers the Spring boot application
     *
     * @param args command line args
     */
    public static void main(String[] args) {
        SpringApplication.run(EmailCertificationApplication.class, args);
    }

    /**
     * Converts the incoming json to readable format and certifies the email data received from fields data and updates it with corresponding status
     * and sends response.
     * 
     * @param message Incoming message published to kafka topic
     * @return json with updated fieldsData.
     * @throws JsonProcessingException when conversion to json is not possible
     */
    @Transformer(inputChannel = Processor.INPUT, outputChannel = Processor.OUTPUT)
    public Map<String, Object> performEmailCertification(Message<?> message) throws JsonProcessingException {
        log.debug("Start of Email certification: \n");
        JsonNode inputJsonNode = mapper.readTree((String) message.getPayload());
        Map<String, Object> input = mapper.convertValue(inputJsonNode, new TypeReference<Map<String, Object>>() {
        });
        input.put(StringUtils.IN_TIME, System.currentTimeMillis());
        log.debug("Input details for Email certification are: \n " + input);
        if (isInputValid(input)) {
            Map<String, Object> fieldsData = mapper.convertValue(input.get(FIELDS_DATA), new TypeReference<Map<String, Object>>() {
            });
            String res = emailCertification.certifyEmail(fieldsData);
            if (VALID_STATUS.equals(res)) {
                input.put("email_certification", "done");
            } else {
                input.put("email_certification", "failed");
                log.error("Email certification failed for email: " + fieldsData.get(EMAIL));
            }
            input.put(FIELDS_DATA, fieldsData);
        } else {
            log.error("Input is not valid.");
        }

        input.put(StringUtils.JOB_TYPE, "email_certification");
        input.put(StringUtils.OUT_TIME, System.currentTimeMillis());
        return input;
    }

    /**
     * Validates the input received.
     * 
     * @param input incoming request json
     * @return boolean, true if input is valid
     */
    private boolean isInputValid(Map<String, Object> input) {
        if (Objects.isNull(input) || Objects.isNull(input.get(FIELDS_DATA))) {
            return false;
        }
        return true;
    }
}
