package com.sproutloud.starter.stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Runs the integration tests for LeaseSegmentRouter Application.
 * 
 * @author mgande
 *
 */
@SpringBootTest(properties = "logging.level.root=DEBUG")
class LeaseSegmentRouterApplicationTests {

    @Autowired
    private MultiOutputProcessor processor;

    @Autowired
    private ObjectMapper mapper;

    @Autowired
    protected MessageCollector collector;

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();

    @BeforeEach
    void init() {
        System.setOut(new PrintStream(out));
    }

    /**
     * Integration tests for the Lease Segment Router.
     * 
     * @throws InterruptedException    if interrupted while polling the response
     * @throws JsonProcessingException if unable to parse response
     */
    @Test
    void testPerformLeaseSegmentRouting() throws InterruptedException, JsonProcessingException {
        String input = "{\n" + 
                "    \"account_id\": \"AC20060000000002\",\n" + 
                "    \"list_id\": \"LI20070000000014\",\n" + 
                "    \"recipient_id\": \"RC20070000000001\",\n" + 
                "    \"locality_code\": \"SL_US\",\n" + 
                "    \"target_db\": \"lm2_dev\", \n" + 
                "    \"created_by\": \"sluser\",\n" + 
                "    \"modified_by\": \"sluser\",\n" + 
                "    \"modified_op\": \"I\",\n" + 
                "    \"lease_data\": {\n" + 
                "        \"target_table\": \"lease_data_ac20060000000001\",\n" + 
                "        \"tp_ids\": [\n" + 
                "            \"tp1\",\n" + 
                "            \"tp2\"\n" + 
                "        ]\n" + 
                "    },\n" + 
                "    \"segment_data\": {\n" + 
                "        \"target_table\": \"list_set_data_ac20060000000002\",\n" + 
                "        \"segments\": [\n" + 
                "            \"LI20070000000012\",\n" + 
                "            \"LI20070000000013\"\n" + 
                "        ]\n" + 
                "    }\n" + 
                "}";
        processor.routerInput().send(MessageBuilder.withPayload(input).build());
        Message<?> lease = collector.forChannel(processor.leaseOutput()).poll(60, TimeUnit.SECONDS);
        JsonNode leaseNode = mapper.readTree((String) Objects.requireNonNull(lease).getPayload());
        Map<String, Object> leaseOutput = mapper.convertValue(leaseNode, new TypeReference<Map<String, Object>>() {
        });
        System.err.println(leaseOutput);
        Message<?> segment = collector.forChannel(processor.segmentOutput()).poll(60, TimeUnit.SECONDS);
        JsonNode segmentNode = mapper.readTree((String) Objects.requireNonNull(segment).getPayload());
        Map<String, Object> segmentOutput = mapper.convertValue(segmentNode, new TypeReference<Map<String, Object>>() {
        });
        System.err.println(segmentOutput);
        assertNotNull(leaseOutput);
        assertNotNull(segmentOutput);
    }

    /**
     * Verifies that lease data is not published to topic when lease data in input is not valid.
     * 
     * @throws InterruptedException    if interrupted while polling the response
     * @throws JsonProcessingException if unable to parse response
     */
    @Test
    void testWithoutLeaseData()  throws InterruptedException, JsonProcessingException {
        String input = "{\n" + 
                "    \"account_id\": \"AC20060000000002\",\n" + 
                "    \"list_id\": \"LI20070000000014\",\n" + 
                "    \"recipient_id\": \"RC20070000000001\",\n" + 
                "    \"locality_code\": \"SL_US\",\n" + 
                "    \"target_db\": \"lm2_dev\", \n" + 
                "    \"created_by\": \"sluser\",\n" + 
                "    \"modified_by\": \"sluser\",\n" + 
                "    \"modified_op\": \"I\",\n" + 
                "    \"segment_data\": {\n" + 
                "        \"target_table\": \"list_set_data_ac20060000000002\",\n" + 
                "        \"segments\": [\n" + 
                "            \"LI20070000000012\",\n" + 
                "            \"LI20070000000013\"\n" + 
                "        ]\n" + 
                "    }\n" + 
                "}";
        processor.routerInput().send(MessageBuilder.withPayload(input).build());
        Message<?> segment = collector.forChannel(processor.segmentOutput()).poll(60, TimeUnit.SECONDS);
        JsonNode segmentNode = mapper.readTree((String) Objects.requireNonNull(segment).getPayload());
        Map<String, Object> segmentOutput = mapper.convertValue(segmentNode, new TypeReference<Map<String, Object>>() {
        });
        System.err.println(segmentOutput);
        assertNotNull(segmentOutput);
        assertThat(out.toString()).contains("Lease data is not present.");
    }

    /**
     * Verifies that lease data is not published to topic when lease data in input does not have table name.
     * 
     * @throws InterruptedException    if interrupted while polling the response
     * @throws JsonProcessingException if unable to parse response
     */
    @Test
    void testWithoutLeaseDataTableName()  throws InterruptedException, JsonProcessingException {
        String input = "{\n" + 
                "    \"account_id\": \"AC20060000000002\",\n" + 
                "    \"list_id\": \"LI20070000000014\",\n" + 
                "    \"recipient_id\": \"RC20070000000001\",\n" + 
                "    \"locality_code\": \"SL_US\",\n" + 
                "    \"target_db\": \"lm2_dev\", \n" + 
                "    \"created_by\": \"sluser\",\n" + 
                "    \"modified_by\": \"sluser\",\n" + 
                "    \"modified_op\": \"I\",\n" + 
                "    \"lease_data\": {\n" + 
                "        \"target_table\": \"\",\n" + 
                "        \"tp_ids\": [\n" + 
                "            \"tp1\",\n" + 
                "            \"tp2\"\n" + 
                "        ]\n" + 
                "    },\n" + 
                "    \"segment_data\": {\n" + 
                "        \"target_table\": \"list_set_data_ac20060000000002\",\n" + 
                "        \"segments\": [\n" + 
                "            \"LI20070000000012\",\n" + 
                "            \"LI20070000000013\"\n" + 
                "        ]\n" + 
                "    }\n" + 
                "}";
        processor.routerInput().send(MessageBuilder.withPayload(input).build());
        Message<?> segment = collector.forChannel(processor.segmentOutput()).poll(60, TimeUnit.SECONDS);
        JsonNode segmentNode = mapper.readTree((String) Objects.requireNonNull(segment).getPayload());
        Map<String, Object> segmentOutput = mapper.convertValue(segmentNode, new TypeReference<Map<String, Object>>() {
        });
        System.err.println(segmentOutput);
        assertNotNull(segmentOutput);
        assertThat(out.toString()).contains("Lease data is not present.");
    }

    /**
     * Verifies that lease data is not published to topic when lease data in input does not have lease list.
     * 
     * @throws InterruptedException    if interrupted while polling the response
     * @throws JsonProcessingException if unable to parse response
     */
    @Test
    void testWithoutLeaseDataLeaseList()  throws InterruptedException, JsonProcessingException {
        String input = "{\n" + 
                "    \"account_id\": \"AC20060000000002\",\n" + 
                "    \"list_id\": \"LI20070000000014\",\n" + 
                "    \"recipient_id\": \"RC20070000000001\",\n" + 
                "    \"locality_code\": \"SL_US\",\n" + 
                "    \"target_db\": \"lm2_dev\", \n" + 
                "    \"created_by\": \"sluser\",\n" + 
                "    \"modified_by\": \"sluser\",\n" + 
                "    \"modified_op\": \"I\",\n" + 
                "    \"lease_data\": {\n" + 
                "        \"target_table\": \"lease_data_ac20060000000001\"\n" + 
                "    },\n" + 
                "    \"segment_data\": {\n" + 
                "        \"target_table\": \"list_set_data_ac20060000000002\",\n" + 
                "        \"segments\": [\n" + 
                "            \"LI20070000000012\",\n" + 
                "            \"LI20070000000013\"\n" + 
                "        ]\n" + 
                "    }\n" + 
                "}";
        processor.routerInput().send(MessageBuilder.withPayload(input).build());
        Message<?> segment = collector.forChannel(processor.segmentOutput()).poll(60, TimeUnit.SECONDS);
        JsonNode segmentNode = mapper.readTree((String) Objects.requireNonNull(segment).getPayload());
        Map<String, Object> segmentOutput = mapper.convertValue(segmentNode, new TypeReference<Map<String, Object>>() {
        });
        System.err.println(segmentOutput);
        assertNotNull(segmentOutput);
        assertThat(out.toString()).contains("Lease data is not present.");
    }

    /**
     * Verifies that segment data is not published to topic when segment data in input is not valid.
     * 
     * @throws InterruptedException    if interrupted while polling the response
     * @throws JsonProcessingException if unable to parse response
     */
    @Test
    void testWithoutSegmentData() throws InterruptedException, JsonProcessingException {
        String input = "{\n" + 
                "    \"account_id\": \"AC20060000000002\",\n" + 
                "    \"list_id\": \"LI20070000000014\",\n" + 
                "    \"recipient_id\": \"RC20070000000001\",\n" + 
                "    \"locality_code\": \"SL_US\",\n" + 
                "    \"target_db\": \"lm2_dev\", \n" + 
                "    \"created_by\": \"sluser\",\n" + 
                "    \"modified_by\": \"sluser\",\n" + 
                "    \"modified_op\": \"I\",\n" + 
                "    \"lease_data\": {\n" + 
                "        \"target_table\": \"lease_data_ac20060000000001\",\n" + 
                "        \"tp_ids\": [\n" + 
                "            \"tp1\",\n" + 
                "            \"tp2\"\n" + 
                "        ]\n" + 
                "    }\n" + 
                "}";
        processor.routerInput().send(MessageBuilder.withPayload(input).build());
        Message<?> lease = collector.forChannel(processor.leaseOutput()).poll(60, TimeUnit.SECONDS);
        JsonNode leaseNode = mapper.readTree((String) Objects.requireNonNull(lease).getPayload());
        Map<String, Object> leaseOutput = mapper.convertValue(leaseNode, new TypeReference<Map<String, Object>>() {
        });
        System.err.println(leaseOutput);
        assertNotNull(leaseOutput);
        assertThat(out.toString()).contains("Segment data is not present.");
    }

    /**
     * Verifies that segment data is not published to topic when segment data in input does not have table name.
     * 
     * @throws InterruptedException    if interrupted while polling the response
     * @throws JsonProcessingException if unable to parse response
     */
    @Test
    void testWithoutSegmentDataTable() throws InterruptedException, JsonProcessingException {
        String input = "{\n" + 
                "    \"account_id\": \"AC20060000000002\",\n" + 
                "    \"list_id\": \"LI20070000000014\",\n" + 
                "    \"recipient_id\": \"RC20070000000001\",\n" + 
                "    \"locality_code\": \"SL_US\",\n" + 
                "    \"target_db\": \"lm2_dev\", \n" + 
                "    \"created_by\": \"sluser\",\n" + 
                "    \"modified_by\": \"sluser\",\n" + 
                "    \"modified_op\": \"I\",\n" + 
                "    \"lease_data\": {\n" + 
                "        \"target_table\": \"lease_data_ac20060000000001\",\n" + 
                "        \"tp_ids\": [\n" + 
                "            \"tp1\",\n" + 
                "            \"tp2\"\n" + 
                "        ]\n" + 
                "    },\n" + 
                "    \"segment_data\": {\n" + 
                "        \"segments\": [\n" + 
                "            \"LI20070000000012\",\n" + 
                "            \"LI20070000000013\"\n" + 
                "        ]\n" + 
                "    }\n" +
                "}";
        processor.routerInput().send(MessageBuilder.withPayload(input).build());
        Message<?> lease = collector.forChannel(processor.leaseOutput()).poll(60, TimeUnit.SECONDS);
        JsonNode leaseNode = mapper.readTree((String) Objects.requireNonNull(lease).getPayload());
        Map<String, Object> leaseOutput = mapper.convertValue(leaseNode, new TypeReference<Map<String, Object>>() {
        });
        System.err.println(leaseOutput);
        assertNotNull(leaseOutput);
        assertThat(out.toString()).contains("Segment data is not present.");
    }

    /**
     * Verifies that segment data is not published to topic when segment data in input does not have segment list.
     * 
     * @throws InterruptedException    if interrupted while polling the response
     * @throws JsonProcessingException if unable to parse response
     */
    @Test
    void testWithoutSegments() throws InterruptedException, JsonProcessingException {
        String input = "{\n" + 
                "    \"account_id\": \"AC20060000000002\",\n" + 
                "    \"list_id\": \"LI20070000000014\",\n" + 
                "    \"recipient_id\": \"RC20070000000001\",\n" + 
                "    \"locality_code\": \"SL_US\",\n" + 
                "    \"target_db\": \"lm2_dev\", \n" + 
                "    \"created_by\": \"sluser\",\n" + 
                "    \"modified_by\": \"sluser\",\n" + 
                "    \"modified_op\": \"I\",\n" + 
                "    \"lease_data\": {\n" + 
                "        \"target_table\": \"lease_data_ac20060000000001\",\n" + 
                "        \"tp_ids\": [\n" + 
                "            \"tp1\",\n" + 
                "            \"tp2\"\n" + 
                "        ]\n" + 
                "    },\n" + 
                "    \"segment_data\": {\n" + 
                "        \"target_table\": \"list_set_data_ac20060000000002\"\n" + 
                "    }\n" + 
                "}";
        processor.routerInput().send(MessageBuilder.withPayload(input).build());
        Message<?> lease = collector.forChannel(processor.leaseOutput()).poll(60, TimeUnit.SECONDS);
        JsonNode leaseNode = mapper.readTree((String) Objects.requireNonNull(lease).getPayload());
        Map<String, Object> leaseOutput = mapper.convertValue(leaseNode, new TypeReference<Map<String, Object>>() {
        });
        System.err.println(leaseOutput);
        assertNotNull(leaseOutput);
        assertThat(out.toString()).contains("Segment data is not present.");
    }
}
