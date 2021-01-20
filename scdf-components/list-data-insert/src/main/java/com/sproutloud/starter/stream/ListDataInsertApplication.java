package com.sproutloud.starter.stream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import static com.sproutloud.starter.stream.util.ApplicationConstants.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;

/**
 * Saves data in input json to cockroach database
 * <p>
 * Annotations used:
 * Slf4j : To generate a logger field
 * EnableBinding(Sink.class) : Registered as a {@link Processor} application in 'Spring cloud data flow' stream
 * EnableJdbcRepositories : To enable JDBC repositories
 *
 * @author sgoyal
 */

@Slf4j
@EnableBinding(Processor.class)
@SpringBootApplication
public class ListDataInsertApplication {

    @Autowired
    private ObjectMapper mapper;
    @Autowired
    private InsertionService dbService;

    /**
     * Triggers the simple spring boot application
     *
     * @param args command line args
     */
    public static void main(String[] args) {
        SpringApplication.run(ListDataInsertApplication.class, args);
    }

    /**
     * Triggers the required queries to save the incoming data to the database with given connection details in the
     * application
     *
     * @param payload incoming {@link org.springframework.messaging.Message} payload containing the data to be saved
     *                to given database
     * @return
     */
    @StreamListener(Processor.INPUT)
    @SendTo(Processor.OUTPUT)
    public Map<String, Object> saveToDb(String payload) throws IOException, SQLException {
        JsonNode jsonNode = mapper.readTree(payload);
        Map<String, Object> input = mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
        });
        input.put(StringUtils.IN_TIME, System.currentTimeMillis());
        parseAndSave(input);
        return createResponse(input);
    }

    /**
     * Creates a response json for the application
     *
     * @param input incoming json message
     * @return output response created by formatting the input
     */
    private Map<String, Object> createResponse(Map<String, Object> input) {
        Map<String, Object> fieldsData = mapper.convertValue(input.get(FIELDS_DATA), new TypeReference<Map<String, Object>>() {
        });
        input.put(LIST_ID, fieldsData.get(LIST_ID));
        input.put(RECIPIENT_ID, fieldsData.get(RECIPIENT_ID));
        input.put(LOCALITY_CODE, fieldsData.get(LOCALITY_CODE));
        input.put(CREATED_BY, fieldsData.get(CREATED_BY));
        input.put(MODIFIED_BY, fieldsData.get(MODIFIED_BY));
        input.put(MODIFIED_OP, fieldsData.get(MODIFIED_OP));
        input.put(JOB_ID, fieldsData.get(JOB_ID));

        input.remove(TABLE);
        input.remove(FIELDS_DATA);
        input.remove(ROUTER_FLAG);
        input.put(StringUtils.JOB_TYPE, "data_ingestion");
        input.put(StringUtils.OUT_TIME, System.currentTimeMillis());
        return input;
    }

    /**
     * Parses the input Map<String><String> and initiates the save to database
     *
     * @param input incoming Map<String><String> with fields data
     * @throws IOException exception occurred while trying to parsing input json for required data
     */
    private void parseAndSave(Map<String, Object> input) throws SQLException {
        if (Objects.nonNull(input.get(TABLE))) {
            String db = null;
            if (Objects.nonNull(input.get(DATABASE)))
                db = (String) input.get(DATABASE);
            String table = (String) input.get(TABLE);
            Map<String, Object> columnMap = mapper.convertValue(input.get(FIELDS_DATA), new TypeReference<Map<String, Object>>() {
            });
            if (!CollectionUtils.isEmpty(columnMap)) {
                dbService.persistToDb(db, table, columnMap);
            }
        } else {
            log.error("Database and table name are missing in the input json");
        }
    }
}