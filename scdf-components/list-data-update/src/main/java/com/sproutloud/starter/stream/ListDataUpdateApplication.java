package com.sproutloud.starter.stream;

import static com.sproutloud.starter.stream.constants.ApplicationConstants.*;

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

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;

/**
 * Updates the entry in cockroach database with data in input json.
 * 
 * @author mgande
 */
@Log4j2
@EnableBinding(Processor.class)
@SpringBootApplication
public class ListDataUpdateApplication {

    @Autowired
    private ObjectMapper mapper;

    @Autowired
    private ListUpdateService updateService;

    @Autowired
    private Processor processor;

    /**
     * Triggers the simple spring boot application
     *
     * @param args command line args
     */
    public static void main(String[] args) {
        SpringApplication.run(ListDataUpdateApplication.class, args);
    }

    /**
     * Triggers the required queries to update the incoming data to the database with given connection details in the application.
     *
     * @param message incoming {@link Message} payload containing the data to be updated. to given database
     * @return output after updating the data.
     */
    @StreamListener(Processor.INPUT)
    public void updateListData(Message<?> message) throws IOException, SQLException {
        JsonNode inputNode = mapper.readTree((String) message.getPayload());
        Map<String, Object> input = mapper.convertValue(inputNode, new TypeReference<Map<String, Object>>() {
        });
        input.put(StringUtils.IN_TIME, System.currentTimeMillis());
        log.debug("Incoming message for List Data update is: \n" + input);
        updateService.updateList(input);
        sendOutput(input);
    }

    /**
     * Creates a response json for the application
     *
     * @param input incoming json message
     * @return output response created by formatting the input
     */
    private void sendOutput(Map<String, Object> input) {
        Map<String, Object> fieldsData = mapper.convertValue(input.get(FIELDS_DATA), new TypeReference<Map<String, Object>>() {
        });
        Map<String, Object> dbData = mapper.convertValue(input.get(DB_FIELDS), new TypeReference<Map<String, Object>>() {
        });
        input.put(LIST_ID, fieldsData.get(LIST_ID));
        input.put(RECIPIENT_ID, dbData.get(RECIPIENT_ID));
        input.put(LOCALITY_CODE, fieldsData.get(LOCALITY_CODE));
        input.put(CREATED_BY, fieldsData.get(CREATED_BY));
        input.put(MODIFIED_BY, fieldsData.get(MODIFIED_BY));
        input.put(MODIFIED_OP, fieldsData.get(MODIFIED_OP));
        input.put(JOB_ID, fieldsData.get(JOB_ID));

        input.remove(TABLE);
        input.remove(FIELDS_DATA);
        input.remove(ROUTER_FLAG);
        input.remove(DB_FIELDS);
        input.put(StringUtils.JOB_TYPE, "data_ingestion");
        input.put(StringUtils.OUT_TIME, System.currentTimeMillis());
        processor.output().send(MessageBuilder.withPayload(input).build());
    }

}
