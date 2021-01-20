package com.sproutloud.starter.stream;

import com.sproutloud.starter.stream.dao.InsertionDao;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * Integratiion tests for data insertion in database
 *
 * @author sgoyal
 */
@SpringBootTest(
        properties = {
                "spring.main.banner-mode=off",
                "spring.datasource.url=jdbc:postgresql://localhost:26257/lm?prepareThreshold=0",
                "spring.datasource.username=sluser",
                "spring.datasource.password=sproutloud",
                "spring.datasource.platform=postgresql",
                "logging.level.org.springframework.jdbc.core = TRACE"
        },
        webEnvironment = SpringBootTest.WebEnvironment.NONE)
class ListDataInsertApplicationTests {

    @Autowired
    private Processor processor;

    @Autowired
    private InsertionDao dao;

    @Autowired
    private MessageCollector collector;

    @Test
    @Tag("contextTest")
    void contextLoads() {
        assertNotNull(processor);
    }

    /**
     * Tests sql array creation of recipient source and job_id values
     *
     * @throws SQLException
     */
    @Test
    void testRecipientSourceAndJobId() throws SQLException {
        Map<String, Object> columnMap = new HashMap<>();
        columnMap.put("job_id", "SJ20200000000056");
        columnMap.put("recipient_source", "USER");

        String input = (String) columnMap.get("job_id");
        dao.createArray(input.split(","));

        columnMap.put("job_id", "SJ20200000000056, SJ20200000000057");
        input = (String) columnMap.get("job_id");
        dao.createArray(input.split(","));

        input = (String) columnMap.get("recipient_source");
        dao.createArray(input.split(","));
    }

    /**
     * Integration testing with the given input and checks for the output message.
     * Deletes the data inserted once test is completed.
     *
     * @throws InterruptedException
     */
    @Test
    @Tag("integrationTest")
    void testListDataInsert() throws InterruptedException, SQLException {
        String content = "{\n" +
                "    \"target_table\": \"list_data_ac20060000000002\",\n" +
                "    \"target_db\": \"lm\",\n" +
                "    \"account_id\": \"AC20060000000002\",\n" +
                "    \"fields_data\": {\n" +
                "        \"first_name\": \"Naveen\",\n" +
                "        \"email\": \"nave@tes.cd\",\n" +
                "        \"address1\": \"AFF\",\n" +
                "        \"address2\": \"AEE\",\n" +
                "        \"city\": \"HYY\",\n" +
                "        \"state\": \"JJU\",\n" +
                "        \"zip\": \"KKOI\",\n" +
                "        \"mobile\": \"9899\",\n" +
                "        \"recipient_id\": \"testRC\",\n" +
                "        \"list_id\": \"LI20090000000004\",\n" +
                "        \"locality_code\": \"SL_US\",\n" +
                "        \"job_id\": \"SJ20200000000056\",\n" +
                "        \"recipient_source\": \"USER\",\n" +
                "        \"created_by\": \"sluser\",\n" +
                "        \"modified_by\": \"sluser\",\n" +
                "        \"modified_op\": \"I\",\n" +
                "        \"email_status\": \"VALID\",\n" +
                "        \"email_message\": \"Valid Email address\",\n" +
                "        \"sms_status\": \"VALID\",\n" +
                "        \"sms_message\": \"Valid phone number\",\n" +
                "        \"mail_status\": \"VALID\",\n" +
                "        \"mail_message\": \"Valid address\",\n" +
                "        \"dedupe_hash\": \"we32dsf3\"\n" +
                "    },\n" +
                "    \"lease_data\": {\n" +
                "        \"target_table\": \"lease_data_ac20060000000012\",\n" +
                "        \"tp_accounts\": [\n" +
                "            \"tp1\",\n" +
                "            \"tp2\"\n" +
                "        ]\n" +
                "    },\n" +
                "    \"segment_data\": {\n" +
                "        \"target_table\": \"list_set_data_ac20060000000012\",\n" +
                "        \"segments\": [\n" +
                "            \"LI20070000000012\",\n" +
                "            \"LI20070000000013\"\n" +
                "        ]\n" +
                "    },\n" +
                "    \"router_flag\": \"insert\"\n" +
                "}";
        processor.input().send(MessageBuilder.withPayload(content).build());
        BlockingQueue<Message<?>> messages = collector.forChannel(processor.output());
        assertNotNull(messages);
        assertNotNull(messages.take().getPayload());
        dao.getJdbcTemplateForTest().execute("DELETE FROM lm.list_data_ac20060000000002 where list_id='LI20090000000004' and locality_code='SL_US' and recipient_id='testRC'");
    }

}
