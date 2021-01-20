package com.sproutloud.starter.stream;

import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;

/**
 * Defines multiple output channels for dedupe input
 * 
 * @author rgupta
 *
 */
public interface DedupeSplitProcessor {

    /**
     * Input channel name.
     */
    String INPUT = "input";

    /**
     * @return input channel.
     */
    @Input(Sink.INPUT)
    SubscribableChannel input();

    /**
     * push data to list-data-insert-channel channel in case of insert
     * 
     * @return
     */
    @Output("list-data-insert-channel")
    MessageChannel insert();

    /**
     * push data to list-data-update-channel channel in case of update
     * 
     * @return
     */
    @Output("list-data-update-channel")
    MessageChannel update();

}
