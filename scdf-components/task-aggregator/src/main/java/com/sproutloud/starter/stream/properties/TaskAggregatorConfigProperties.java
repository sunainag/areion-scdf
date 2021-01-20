package com.sproutloud.starter.stream.properties;

import lombok.Getter;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * Redis properties file.
 * 
 * @author mgande
 *
 */
@Configuration
@Getter
public class TaskAggregatorConfigProperties {

    /**
     * Redis host name.
     */
    @Value("${spring.redis.host:10.1.66.66}")
    private String host;

    /**
     * Redis port.
     */
    @Value("${spring.redis.port:6379}")
    private Integer port;

    /**
     * Redis password.
     */
    @Value("${spring.redis.password:y#5wadrUh@wR}")
    private String password;
}
