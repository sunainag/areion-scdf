package com.sproutloud.starter.stream;

import static com.sproutloud.starter.stream.constants.ApplicationConstants.JOB_ID;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.LEASE_DATA;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.LIST_ID;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.RECIPIENT_ID;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.SEGMENTS;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.SEGMENT_DATA;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.TARGET_TABLE;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.TP_IDS;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.log4j.Log4j2;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 
 * Separates the incoming message to lease and segment data and sends details to appropriate channels.
 * 
 * @author mgande
 *
 */
@Log4j2
@EnableBinding(MultiOutputProcessor.class)
@SpringBootApplication
public class LeaseSegmentRouterApplication {

    /**
     * {@link ObjectMapper} for converting incoming message to required type.
     */
    @Autowired
    private ObjectMapper mapper;

    /**
     * {@link Processor} for routing lease and segment data.
     */
    @Autowired
    private MultiOutputProcessor processor;

    /**
     * Triggers the Spring boot application
     *
     * @param args command line args
     */
    public static void main(String[] args) {
        SpringApplication.run(LeaseSegmentRouterApplication.class, args);
    }

    /**
     * Converts the incoming json to readable format. 
     * Routes/sends the lease and segment data to the respective output channel.
     *
     * @param message incoming message to {@link MultiOutputProcessor}
     * @throws JsonProcessingException if fails to parse json
     */
    @StreamListener(MultiOutputProcessor.INPUT)
    public void performLeaseSegmentRouting(Message<?> message) throws JsonProcessingException {
        log.debug("Start of Lease and Segment routing: \n");
        JsonNode inputJsonNode = mapper.readTree((String) message.getPayload());
        Map<String, Object> input = mapper.convertValue(inputJsonNode, new TypeReference<Map<String, Object>>() {
        });
        boolean lease = false;
        boolean segment = false;
        log.debug("Input details for Lease Segment Router are: \n " + input);
        if (Objects.nonNull(input)) {
            Map<String, Object> leaseData = mapper.convertValue(input.get(LEASE_DATA), new TypeReference<Map<String, Object>>() {
            });
            Map<String, Object> segmentData = mapper.convertValue(input.get(SEGMENT_DATA), new TypeReference<Map<String, Object>>() {
            });
            input.remove(LEASE_DATA);
            input.remove(SEGMENT_DATA);
            lease = sendLeaseData(input, leaseData);
            segment = sendSegmentData(input, segmentData);
        } else {
            log.error("Input is not valid.");
        }

        log.debug("Sending details to data aggregator channel.");
        Map<String, Object> aggOutput = new HashMap<>();
        aggOutput.put(com.sproutloud.starter.stream.StringUtils.IN_TIME, input.get(com.sproutloud.starter.stream.StringUtils.IN_TIME));
        aggOutput.put(com.sproutloud.starter.stream.StringUtils.JOB_TYPE, "data_ingestion");
        aggOutput.put(JOB_ID, input.get(JOB_ID));
        aggOutput.put(com.sproutloud.starter.stream.StringUtils.OUT_TIME, System.currentTimeMillis());
        aggOutput.put(LIST_ID, input.get(LIST_ID));
        aggOutput.put(RECIPIENT_ID, input.get(RECIPIENT_ID));
        aggOutput.put("lease_data_ingestion_required", lease);
        aggOutput.put("segment_data_ingestion_required", segment);
        processor.aggregatorOutput().send(MessageBuilder.withPayload(aggOutput).build());
    }

    /**
     * Validates the incoming lease data and sends lease data to respective channel.
     * 
     * @param input     incoming message data.
     * @param leaseData to be sent to lease channel.
     * @return true if lease data exists
     */
    private boolean sendLeaseData(Map<String, Object> input, Map<String, Object> leaseData) {
        if (!CollectionUtils.isEmpty(leaseData) && !StringUtils.isEmpty(leaseData.get(TARGET_TABLE))
                && !StringUtils.isEmpty(leaseData.get(TP_IDS))) {
            input.put(TARGET_TABLE, leaseData.get(TARGET_TABLE));
            input.put(TP_IDS, leaseData.get(TP_IDS));
            input.put(com.sproutloud.starter.stream.StringUtils.JOB_TYPE, "to_lease_data_ingestion");
            input.put(com.sproutloud.starter.stream.StringUtils.OUT_TIME, System.currentTimeMillis());
            processor.leaseOutput().send(MessageBuilder.withPayload(input).build());
            return true;
        } else {
            log.debug("Lease data is not present.");
            return false;
        }
    }

    /**
     * Validates the incoming segment data and sends segment data to respective channel.
     * 
     * @param input       incoming message data.
     * @param segmentData to be sent to segment channel.
     * @return true if segment data exists
     */
    private boolean sendSegmentData(Map<String, Object> input, Map<String, Object> segmentData) {
        if (!CollectionUtils.isEmpty(segmentData) && !StringUtils.isEmpty(segmentData.get(TARGET_TABLE))
                && !StringUtils.isEmpty(segmentData.get(SEGMENTS))) {
            input.remove(TP_IDS);
            input.put(TARGET_TABLE, segmentData.get(TARGET_TABLE));
            input.put(SEGMENTS, segmentData.get(SEGMENTS));
            input.put(com.sproutloud.starter.stream.StringUtils.JOB_TYPE, "to_segment_data_ingestion");
            input.put(com.sproutloud.starter.stream.StringUtils.OUT_TIME, System.currentTimeMillis());
            processor.segmentOutput().send(MessageBuilder.withPayload(input).build());
            return true;
        } else {
            log.debug("Segment data is not present.");
            return false;
        }
    }

}
