package com.sproutloud.starter.stream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.log4j.Log4j2;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * application to redirect insert/update traffic to respective topic after
 * dedupe
 * 
 * @author rgupta
 *
 */
@Log4j2
@SpringBootApplication
@EnableBinding(DedupeSplitProcessor.class)
public class DedupeInsUpdRouterApplication {

    @Autowired
    ObjectMapper mapper;

    @Autowired
    private DedupeSplitProcessor processor;

    /**
     * main method of the application
     * 
     * @param args
     */
    public static void main(String[] args) {
        SpringApplication.run(DedupeInsUpdRouterApplication.class, args);
    }

    /**
     * Processes the input record and redirects to appropriate kafka topic
     * 
     * @param message
     * @throws IOException
     */
    @StreamListener(target = DedupeSplitProcessor.INPUT)
    public void dedupeInsertUpdateRouter(Message<?> message) throws IOException {
        JsonNode jsonNode = mapper.readTree((String) message.getPayload());
        Map<String, Object> input = mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
        });
        log.debug("********* Processing Dedupe Split : ******************* \n" + input);
        if (Objects.equals(input.get("router_flag").toString().toUpperCase(), "INSERT")) {
            log.info("Sending Message to insert the row");
            processor.insert().send(MessageBuilder.withPayload(input).build());
        } else if (Objects.equals(input.get("router_flag").toString().toUpperCase(), "UPDATE")) {
            log.info("Sending Message to update the row");
            processor.update().send(MessageBuilder.withPayload(input).build());
        }

    }

}
