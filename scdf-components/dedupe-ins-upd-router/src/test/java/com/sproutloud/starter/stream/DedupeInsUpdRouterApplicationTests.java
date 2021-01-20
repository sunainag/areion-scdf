package com.sproutloud.starter.stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.messaging.support.MessageBuilder;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Tests the functionality of the application
 * 
 * @author rgupta
 *
 */
@SpringBootTest
class DedupeInsUpdRouterApplicationTests {

    /**
     * Processor object to be used
     */
    @Autowired
    protected DedupeSplitProcessor channels;

    /**
     * mapper object to be used
     */
    @Autowired
    protected ObjectMapper mapper;

    /**
     * Tests if the context if loaded or not
     */
    @Test
    void contextLoads() {
        assertNotNull(channels);
    }

    /**
     * Checks for router_flag insert passed in upper case
     * 
     * @throws InterruptedException
     * @throws JsonParseException
     * @throws JsonMappingException
     * @throws IOException
     */
    @Test
    void testInsertUpperCase() throws InterruptedException, JsonParseException, JsonMappingException, IOException {
        List<String> tpAccounts = new ArrayList<>();
        tpAccounts.add("AC20070000000022");
        tpAccounts.add("AC20070000000023");

        Map<String, Object> input = new HashMap<>();
        input.put("target_db", "lm2_dev");
        input.put("account_id", "AC20060000000002");
        input.put("modified_by", "rgupta");
        input.put("created_by", "rgupta");
        input.put("modified_op", "I");
        input.put("locality_code", "SL_US");
        input.put("list_id", "LI20070000000014");
        input.put("recipient_id", "RC20070000001206");
        input.put("tp_ids", tpAccounts);
        input.put("router_flag", "INSERT");

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        System.setOut(new PrintStream(out));
        channels.input().send(MessageBuilder.withPayload(mapper.writeValueAsString(input)).build());
        assertThat(out.toString()).contains("Sending Message to insert the row");
    }

    /**
     * Checks for router_flag insert passed in lower case
     * 
     * @throws InterruptedException
     * @throws JsonParseException
     * @throws JsonMappingException
     * @throws IOException
     */
    @Test
    void testInsertLowerCase() throws InterruptedException, JsonParseException, JsonMappingException, IOException {
        List<String> tpAccounts = new ArrayList<>();
        tpAccounts.add("AC20070000000022");
        tpAccounts.add("AC20070000000023");

        Map<String, Object> input = new HashMap<>();
        input.put("target_db", "lm2_dev");
        input.put("account_id", "AC20060000000002");
        input.put("modified_by", "rgupta");
        input.put("created_by", "rgupta");
        input.put("modified_op", "I");
        input.put("locality_code", "SL_US");
        input.put("list_id", "LI20070000000014");
        input.put("recipient_id", "RC20070000001206");
        input.put("tp_ids", tpAccounts);
        input.put("router_flag", "insert");

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        System.setOut(new PrintStream(out));
        channels.input().send(MessageBuilder.withPayload(mapper.writeValueAsString(input)).build());
        assertThat(out.toString()).contains("Sending Message to insert the row");
    }

    /**
     * Checks for router_flag update passed in upper case
     * 
     * @throws InterruptedException
     * @throws JsonParseException
     * @throws JsonMappingException
     * @throws IOException
     */
    @Test
    void testUpdate() throws InterruptedException, JsonParseException, JsonMappingException, IOException {
        List<String> tpAccounts = new ArrayList<>();
        tpAccounts.add("AC20070000000022");
        tpAccounts.add("AC20070000000023");

        Map<String, Object> input = new HashMap<>();
        input.put("target_db", "lm2_dev");
        input.put("account_id", "AC20060000000002");
        input.put("modified_by", "rgupta");
        input.put("created_by", "rgupta");
        input.put("modified_op", "I");
        input.put("locality_code", "SL_US");
        input.put("list_id", "LI20070000000014");
        input.put("recipient_id", "RC20070000001206");
        input.put("tp_ids", tpAccounts);
        input.put("router_flag", "UPDATE");

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        System.setOut(new PrintStream(out));
        channels.input().send(MessageBuilder.withPayload(mapper.writeValueAsString(input)).build());
        assertThat(out.toString()).contains("Sending Message to update the row");
    }
}
