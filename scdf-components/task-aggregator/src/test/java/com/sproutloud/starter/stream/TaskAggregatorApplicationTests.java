package com.sproutloud.starter.stream;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.cloud.stream.test.binder.MessageCollector;

/**
 * Runs integration test
 * 
 * @author rgupta
 *
 */
@SpringBootTest
class TaskAggregatorApplicationTests {

    /**
     * processor object to be used
     */
    @Autowired
    protected Sink sink;

    /**
     * MessageCollector object to be used
     */
    @Autowired
    protected MessageCollector collector;

    /**
     * mapper object to be used
     */
    @Autowired
    protected ObjectMapper mapper;

    /**
     * Tests if context is loaded
     */
//    @Test
    void contextLoads() {
//        assertNotNull(sink);
    }

}
