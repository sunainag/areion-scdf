package com.sproutloud.starter.stream.redis;

import com.sproutloud.starter.stream.properties.TaskAggregatorConfigProperties;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisClientConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericToStringSerializer;

/**
 * Redis configuration to be used as intermediate storage
 * 
 * @author rishabhg
 *
 */
@Configuration
public class RedisConfig {

    /**
     * properties object to be used
     */
    @Autowired
    private TaskAggregatorConfigProperties properties;

    /**
     * Creates jedis connection factory for redis connection.
     * 
     * @return {@link JedisConnectionFactory} bean.
     */
    @Bean
    JedisConnectionFactory jedisConnectionFactory() {
        RedisStandaloneConfiguration configuration = new RedisStandaloneConfiguration(properties.getHost(), properties.getPort());
        configuration.setPassword(properties.getPassword());
        JedisClientConfiguration jedisClientConfiguration = JedisClientConfiguration.builder().usePooling().build();
        JedisConnectionFactory factory = new JedisConnectionFactory(configuration, jedisClientConfiguration);
        factory.afterPropertiesSet();
        return factory;
    }

    /**
     * Created {@link RedisTemplate} bean.
     * 
     * @return {@link RedisTemplate} bean.
     */
    @Bean
    public RedisTemplate<String, Object> redisTemplate() {
        final RedisTemplate<String, Object> redisTemplate = new RedisTemplate<>();
        redisTemplate.setConnectionFactory(jedisConnectionFactory());
        redisTemplate.setValueSerializer(new GenericToStringSerializer<Object>(Object.class));
        return redisTemplate;
    }
}
