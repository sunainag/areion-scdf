package com.sproutloud.starter.stream;

import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;

/**
 * Bindable interface with one input channel and multiple output channels.
 *
 * @author mgande
 */
public interface MultiOutputProcessor {

    /**
     * Input channel name.
     */
    String INPUT = "input";

    /**
     * Output channel name for lease data
     */
    String LEASE_OUTPUT = "lease-data-channel";

    /**
     * Output channel name for segment data
     */
    String SEGMENT_OUTPUT = "segment-data-channel";

    /**
     * Output channel name for aggregator channel
     */
    String AGGREGATOR_OUTPUT = "data-agg-channel";

    /**
     * @return input channel.
     */
    @Input(INPUT)
    SubscribableChannel routerInput();

    /**
     * @return lease output channel.
     */
    @Output(LEASE_OUTPUT)
    MessageChannel leaseOutput();

    /**
     * @return segment output channel.
     */
    @Output(SEGMENT_OUTPUT)
    MessageChannel segmentOutput();

    /**
     * @return aggregator output channel.
     */
    @Output(AGGREGATOR_OUTPUT)
    MessageChannel aggregatorOutput();
}
