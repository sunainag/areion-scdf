package com.example.multioutputprocessor;

import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;

public interface MultiOutputProcessor {
  String INPUT = "input";

  @Input
  SubscribableChannel input();

  @Output("lease-data-channel")
  MessageChannel lease();

  @Output("segment-data-channel")
  MessageChannel segment();


}
