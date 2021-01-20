package com.sproutloud.starter.stream;

import static com.sproutloud.starter.stream.constants.ApplicationConstants.FIELDS_DATA;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.MOBILE;
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
 * Spring cloud data flow processor using spring boot. Performs phone certification and sets status accordingly.
 * 
 * @author mgande
 *
 */
@Log4j2
@EnableBinding(Processor.class)
@SpringBootApplication
public class PhoneCertificationApplication {

    /**
     * {@link PhoneCertification} bean for phone certification.
     */
    @Autowired
    private PhoneCertification phoneCertification;

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
        SpringApplication.run(PhoneCertificationApplication.class, args);
    }

    /**
     * Converts the incoming json to readable format and certifies the mobile data received from fields data and updates it with corresponding status
     * and sends response.
     * 
     * @param message Incoming message published to kafka topic
     * @return json with updated fieldsData.
     * @throws JsonProcessingException when conversion to json is not possible
     */
    @Transformer(inputChannel = Processor.INPUT, outputChannel = Processor.OUTPUT)
    public Map<String, Object> performPhoneCertification(Message<?> message) throws JsonProcessingException {
        log.debug("Start of Phone certification: \n");
        JsonNode inputJsonNode = mapper.readTree((String) message.getPayload());
        Map<String, Object> input = mapper.convertValue(inputJsonNode, new TypeReference<Map<String, Object>>() {
        });
        input.put(StringUtils.IN_TIME, System.currentTimeMillis());
        log.debug("Input details for Phone certification are: \n " + input);
        if (isInputValid(input)) {
            Map<String, Object> fieldsData = mapper.convertValue(input.get(FIELDS_DATA), new TypeReference<Map<String, Object>>() {
            });
            String status = phoneCertification.certifyPhone((String)input.get("country_code"), fieldsData);
            if (VALID_STATUS.equals(status)) {
                input.put(StringUtils.PHONE_CERTIFICATION, "done");
            } else {
                log.error("Phone certification failed for mobile: " + fieldsData.get(MOBILE));
                input.put(StringUtils.PHONE_CERTIFICATION, "failed");
            }
            input.put(FIELDS_DATA, fieldsData);
        } else {
            input.put(StringUtils.PHONE_CERTIFICATION, "failed");
            log.error("Input is not valid.");
        }
        input.put(StringUtils.JOB_TYPE, StringUtils.PHONE_CERTIFICATION);
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
