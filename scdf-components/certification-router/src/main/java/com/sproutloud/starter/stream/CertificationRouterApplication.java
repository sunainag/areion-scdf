package com.sproutloud.starter.stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;

import java.util.Map;
import java.util.Objects;

/**
 * Routes and processes the incoming message to the respective MessageChannel, based on the certification data in the
 * input.
 *
 * @author sgoyal
 */
@EnableBinding(MultiOutputProcessor.class)
@SpringBootApplication
public class CertificationRouterApplication {

    @Autowired
    ObjectMapper mapper;
    @Autowired
    MultiOutputProcessor processor;

    /**
     * Triggers the simple spring boot application
     *
     * @param args command line args
     */
    public static void main(String[] args) {
        SpringApplication.run(CertificationRouterApplication.class, args);
    }

    /**
     * Routes/sends the response to the chosen output MessageChannel.
     * Currently, routed based on if the value of email,phone and/or address certifications in the input is 'required'.
     * Default output channel is 'transformation_channel', in case the certification requirements are missing or not
     * 'required'.
     *
     * @param message incoming message to {@link MultiOutputProcessor}
     * @throws JsonProcessingException if fails to parse json
     */
    @StreamListener(MultiOutputProcessor.INPUT)
    public void route(Message<?> message) throws JsonProcessingException {
        JsonNode jsonNode = mapper.readTree((String) Objects.requireNonNull(message).getPayload());
        Map<String, Object> payload = mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
        });
        payload.put(StringUtils.IN_TIME, System.currentTimeMillis());
        routeToChannel(payload);
    }

    /**
     * Routes/sends the response to the respective output MessageChannel. Currently, routed based on if the value of
     * email,phone and/or address certifications in the input is 'required'.
     * If either of the certifications are missing or are not 'required', then by default, the response is sent to
     * 'transformation_channel'.
     * If 'email_certification' is 'required', then send the response to email_certification_channel, and the like.
     * If all the certifications are 'required', then sends response to any of the channels, here, response is sent
     * first to email_certification_channel, if not 'required', then checks for phone_certification_channel and then
     * address_certification_channel.
     * Sends response once the output channel is decided.
     *
     * @param payload incoming json message with certification data
     */
    private void routeToChannel(Map<String, Object> payload) {
        MessageChannel messageChannel = processor.transformationOutput();
        String jobType = "certification_router";
        if (Objects.equals(payload.get(StringUtils.EMAIL_CERTIFICATION), StringUtils.IS_CERTIFICATION_REQUIRED)) {
            messageChannel = processor.emailOutput();
            jobType = "to_email_certification";
        } else if (Objects.equals(payload.get(StringUtils.PHONE_CERTIFICATION),
                StringUtils.IS_CERTIFICATION_REQUIRED)) {
            messageChannel = processor.phoneOutput();
            jobType = "to_phone_certification";
        } else if (Objects.equals(payload.get(StringUtils.ADDRESS_CERTIFICATION),
                StringUtils.IS_CERTIFICATION_REQUIRED)) {
            messageChannel = processor.addressOutput();
            jobType = "to_address_certification";
        }
        payload.put(StringUtils.JOB_TYPE, jobType);
        payload.put(StringUtils.OUT_TIME, System.currentTimeMillis());
        messageChannel.send(MessageBuilder.withPayload(payload).build());
    }
}
