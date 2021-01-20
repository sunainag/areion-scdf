package com.sproutloud.starter.stream;

import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;

/**
 * Bindable interface with one input channel and multiple output channels.
 *
 * @author sgoyal
 * @see org.springframework.cloud.stream.annotation.EnableBinding
 */
public interface MultiOutputProcessor {

    /**
     * Input channel name.
     */
    String INPUT = "input";

    /**
     * Output channel name for email certification
     */
    String EMAIL_OUTPUT = "email-certification-channel";

    /**
     * Output channel name for phone certification
     */
    String PHONE_OUTPUT = "phone-certification-channel";

    /**
     * Output channel name for address certification
     */
    String ADDRESS_OUTPUT = "address-certification-channel";

    /**
     * Output channel name for transformation of data
     */
    String TRANSFORMATION_OUTPUT = "output";

    /**
     * @return input channel.
     */
    @Input(INPUT)
    SubscribableChannel routerInput();

    /**
     * @return email output channel.
     */
    @Output(EMAIL_OUTPUT)
    MessageChannel emailOutput();

    /**
     * @return phone output channel.
     */
    @Output(PHONE_OUTPUT)
    MessageChannel phoneOutput();

    /**
     * @return address output channel.
     */
    @Output(ADDRESS_OUTPUT)
    MessageChannel addressOutput();

    /**
     * @return transformation output channel.
     */
    @Output(TRANSFORMATION_OUTPUT)
    MessageChannel transformationOutput();
}
