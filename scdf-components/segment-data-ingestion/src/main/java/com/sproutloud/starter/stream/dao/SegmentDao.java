package com.sproutloud.starter.stream.dao;

import java.util.List;

/**
 * Used for communication to database for inserting segment data
 * 
 * @author rgupta
 *
 */
public interface SegmentDao {

    /**
     * Runs insert statement for segment data
     * 
     * @param database
     * @param accountId
     * @param columnKeys
     * @param columnValues
     */
    void runSegmentInsert(String database, String accountId, String columnKeys, List<String> columnValues);

}
