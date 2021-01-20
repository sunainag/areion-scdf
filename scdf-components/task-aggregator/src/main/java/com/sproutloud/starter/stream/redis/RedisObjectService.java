package com.sproutloud.starter.stream.redis;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

/**
 * Inserts, fetches and deletes data from redis database.
 * 
 * @author rishabhg
 *
 */
@Service
public class RedisObjectService {

    /**
     * table name to be used for storing data in redis
     */
    private static final String TABLE_NAME = "job_status";

    /**
     * redis template object to be used
     */
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    /**
     * HashOperations object to be used
     */
    private HashOperations<String, String, Object> hashOperations;

    /**
     * Initializes a redis connection
     */
    @PostConstruct
    private void init() {
        hashOperations = redisTemplate.opsForHash();
    }

    /**
     * saves the given object in the table for the given jobId
     * 
     * @param jobId  key of the object to be saved.
     * @param object to be saved.
     */
    public void save(String jobId, Object object) {
        hashOperations.put(TABLE_NAME, jobId, object);
    }

    /**
     * Fetches the data from redis for the givenjobId.
     * 
     * @param jobId id for which data has to be fetched.
     * @return database object obtained from given id.
     */
    public Object getByJobId(String jobId) {
        return hashOperations.get(TABLE_NAME, jobId);
    }

    /**
     * deletes the data from redis for the given jobId.
     * 
     * @param jobId id for which entry has to be deleted.
     */
    public void remove(String jobId) {
        hashOperations.delete(TABLE_NAME, jobId);
    }
}
