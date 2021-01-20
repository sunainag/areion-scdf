package com.sproutloud.starter.stream;

import static com.sproutloud.starter.stream.constants.ApplicationConstants.*;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sproutloud.starter.stream.dao.impl.FieldsDaoImpl;

import lombok.extern.log4j.Log4j2;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Spring cloud data flow processor using spring boot. 
 * Reads the csv from Google storage and normalizes it to required json format.
 * 
 * @author mgande
 *
 */
@Log4j2
@EnableBinding(Processor.class)
@SpringBootApplication
@EnableCaching
public class NormaliseCsvToJsonApplication {

    /**
     * {@link Processor} to send messages to output channel.
     */
    @Autowired
    private Processor processor;

    /**
     * {@link ObjectMapper} to convert Object to required type.
     */
    @Autowired
    private ObjectMapper mapper;

    /**
     * {@link CsvToJsonNormalize} Bean to normalize csv.
     */
    @Autowired
    private CsvToJsonNormalize normalize;

    /**
     * {@link FieldsDaoImpl} for db queries.
     */
    @Autowired
    private FieldsDaoImpl dao;

    /**
     * Triggers the Spring boot application
     *
     * @param args command line args
     */
    public static void main(String[] args) {
        SpringApplication.run(NormaliseCsvToJsonApplication.class, args);
    }

    /**
     * Converts the incoming message (Object) to required json. Reads CSV file from Google storage. Normalizes the CSV data to required json. Sends
     * each entry in CSV to an output channel.
     * 
     * @param message Incoming message published to kafka topic
     * @throws IOException when conversion to json is not possible or unable to read CSV file.
     */
    @StreamListener(Processor.INPUT)
    public void normalizeCsvToJson(Message<?> message) throws IOException {
        log.debug("Starting Normalization of csv to json: \n");
        JsonNode inputNode = mapper.readTree((String) message.getPayload());
        Map<String, Object> input = mapper.convertValue(inputNode, new TypeReference<Map<String, Object>>() {
        });
        input.put(StringUtils.IN_TIME, System.currentTimeMillis());
        log.debug("Input for the Component is : \n " + input);
        if (Objects.nonNull(input) && Objects.nonNull(input.get(FILE)) && Objects.nonNull(input.get(MAPPINGS))) {
            log.debug("Getting mappings and dedupe_fields from input \n ");
            Map<String, Integer> fieldIndexMap = mapper.convertValue(input.get(MAPPINGS), new TypeReference<Map<String, Integer>>() {
            });
            List<String> dedupeFields = mapper.convertValue(input.get(DEDUPE_FIELDS), new TypeReference<List<String>>() {
            });

            log.debug("Checking if all the dedupe fields are in mappings. \n");
            if (!fieldIndexMap.keySet().containsAll(dedupeFields)) {
                throw new RuntimeException("Dedupe field is not present in mappings");
            }
            log.debug("Getting field details of given list_id from DB. \n");
            Map<String, Map<String, String>> fieldDetails = normalize.getFieldDetails(input, fieldIndexMap);
            Map<String, String> segmentSegmentIdMap = new HashMap<>();
            Map<Integer, Map<String, Object>> dedupeHashFieldsDataMap = new HashMap<>();
            Integer totalNumberOfRecords = normalize.normalizeInput(input, dedupeFields, fieldIndexMap, segmentSegmentIdMap, dedupeHashFieldsDataMap);

            Integer numberOfRecords = dedupeHashFieldsDataMap.size();
            Integer sequenceNumber = dao.getSequenceNumber(numberOfRecords, "recipient_seq");
            for (Entry<Integer, Map<String, Object>> field : dedupeHashFieldsDataMap.entrySet()) {
                sendDetailsToOutputChannel(input, field.getValue(), sequenceNumber++, fieldDetails, segmentSegmentIdMap, numberOfRecords, totalNumberOfRecords);
            }
        }
        dao.updateJobStatus(input.get("job_id"), System.currentTimeMillis());
    }

    /**
     * Prepares the output json in the required format with the details given and sends to output channel.
     * 
     * @param input                json format of incoming message
     * @param fieldsData           {@link Map} with key as field-name and values as field-value.
     * @param sequenceNumber       {@link Integer} to generate unique recepient_id.
     * @param fieldDetails         {@link Map} with key as field-name and value as field-details.
     * @param segmentSegmentIdMap  {@link Map} with key as segmentValue and value as segmentId.
     * @param numberOfRecords      number of unique records in csv
     * @param totalNumberOfRecords number of records in csv
     */
    private void sendDetailsToOutputChannel(Map<String, Object> input, Map<String, Object> fieldsData, Integer sequenceNumber,
            Map<String, Map<String, String>> fieldDetails, Map<String, String> segmentSegmentIdMap, Integer numberOfRecords, Integer totalNumberOfRecords) {
        Map<String, Object> leaseData = prepareLeaseDataForOutput(input, fieldsData);

        Map<String, Object> segmentData = prepareSegmentDataForOutput(input, fieldsData, segmentSegmentIdMap);

        log.debug("removing lease and segments details from fieldsData \n");
        fieldsData.remove(TP_IDS);
        fieldsData.remove(SEGMENTS);

        prepareFieldsDataForOutput(input, fieldsData, sequenceNumber);

        log.debug("Preparing output for the channel \n ");
        Map<String, Object> outputData = new HashMap<>();
        outputData.put(TARGET_TABLE, input.get(TARGET_TABLE));
        outputData.put(TARGET_DB, input.get(TARGET_DB));
        outputData.put(ACCOUNT_ID, input.get(ACCOUNT_ID));
        outputData.put(COUNTRY_CODE, input.get(COUNTRY_CODE));
        outputData.put(DEDUPE_FIELDS, input.get(DEDUPE_FIELDS));
        outputData.put(IS_CASS_REQUIRED, input.get(IS_CASS_REQUIRED));
        outputData.put("email_certification", REQUIRED);
        outputData.put("phone_certification", REQUIRED);
        outputData.put("address_certification", REQUIRED);
        outputData.put(FIELDS_DATA, fieldsData);
        outputData.put("number_of_records", numberOfRecords);
        outputData.put("duplicate_records", (totalNumberOfRecords - numberOfRecords));
        if (!leaseData.isEmpty()) {
            outputData.put(LEASE_DATA, leaseData);
        }
        if (!segmentData.isEmpty()) {
            outputData.put(SEGMENT_DATA, segmentData);
        }
        outputData.put(FIELD_DETAILS, fieldDetails);
        outputData.put(StringUtils.IN_TIME, input.get(StringUtils.IN_TIME));
        outputData.put(StringUtils.JOB_TYPE, "csv_normalization");
        outputData.put(StringUtils.OUT_TIME, System.currentTimeMillis());
        processor.output().send(MessageBuilder.withPayload(outputData).build());
    }

    /**
     * Prepares lease data details in the required output format from the fieldsData.
     * 
     * @param input      json format of incoming message
     * @param fieldsData {@link Map} with key as field-name and values as field-value.
     * @return {@link Map} with the lease data details.
     */
    private Map<String, Object> prepareLeaseDataForOutput(Map<String, Object> input, Map<String, Object> fieldsData) {
        log.debug("Preparing lease data \n");
        Map<String, Object> leaseData = new HashMap<>();
        Set<String> leaseSet = mapper.convertValue(fieldsData.get(TP_IDS), new TypeReference<Set<String>>() {
        });
        if (!CollectionUtils.isEmpty(leaseSet)) {
            leaseData.put(TARGET_TABLE, input.get("lease_table"));
            leaseData.put(TP_IDS, leaseSet);
        }
        return leaseData;
    }

    /**
     * Prepares segment data details in the required output format from the fieldsData.
     * 
     * @param input               json format of incoming message
     * @param fieldsData          {@link Map} with key as field-name and values as field-value.
     * @param segmentSegmentIdMap {@link Map} with key as segmentValue and value as segmentId.
     * @return {@link Map} with the segment data details.
     */
    private Map<String, Object> prepareSegmentDataForOutput(Map<String, Object> input, Map<String, Object> fieldsData,
            Map<String, String> segmentSegmentIdMap) {
        log.debug("Preparing segment data \n");
        Map<String, Object> segmentData = new HashMap<>();
        List<String> list = getSegmentIdsFromSegments(fieldsData.get(SEGMENTS), segmentSegmentIdMap);
        if (!list.isEmpty()) {
            segmentData.put(TARGET_TABLE, input.get("segment_table"));
            segmentData.put(SEGMENTS, list);
        }
        return segmentData;
    }

    /**
     * Prepares list of segmentsIds corresponding to the segments from {@link Map} segmentSegmentIdMap.
     * 
     * @param segments            Object with list of segment strings
     * @param segmentSegmentIdMap {@link Map} with key as segmentValue and value as segmentId.
     * @return {@link List} of segmentIds mapped to segments.
     */
    private List<String> getSegmentIdsFromSegments(Object segments, Map<String, String> segmentSegmentIdMap) {
        Set<String> segmentsSet = mapper.convertValue(segments, new TypeReference<Set<String>>() {
        });
        return segmentsSet.stream().map(segment -> segmentSegmentIdMap.get(segment)).collect(Collectors.toList());
    }

    /**
     * Updates the {@link Map} fieldsData as per the output format required.
     * 
     * @param input          json format of incoming message
     * @param fieldsData     {@link Map} with key as field-name and values as field-value.
     * @param sequenceNumber {@link Integer} to generate unique recepient_id.
     */
    private void prepareFieldsDataForOutput(Map<String, Object> input, Map<String, Object> fieldsData, Integer sequenceNumber) {
        log.debug("Adding details to fields_data \n ");
        fieldsData.put(JOB_ID, input.get(JOB_ID));
        fieldsData.put(RECIPIENT_SOURCE, input.get(RECIPIENT_SOURCE));
        fieldsData.put(LIST_ID, input.get(LIST_ID));
        fieldsData.put(RECIPIENT_ID, normalize.getId(sequenceNumber, "RC"));
        fieldsData.put(LOCALITY_CODE, "SL_US");
        fieldsData.put("email_status", "");
        fieldsData.put("email_message", "");
        fieldsData.put("sms_status", "");
        fieldsData.put("sms_message", "");
        fieldsData.put("mail_status", "");
        fieldsData.put("mail_message", "");
        fieldsData.put("modified_op", "I");
        fieldsData.put("created_by", input.get(USER));
        fieldsData.put("modified_by", input.get(USER));
    }
}
