package com.sproutloud.starter.stream.dao;

import java.util.List;

/**
 * Used for communication to database for inserting lease data
 * 
 * @author rgupta
 *
 */
public interface LeaseDao {

    /**
     * Runs insert statement for lease data
     * 
     * @param database
     * @param accountId
     * @param columnKeys
     * @param columnValues
     */
    void runLeaseInsert(String database, String accountId, String columnKeys, List<String> columnValues);

}
