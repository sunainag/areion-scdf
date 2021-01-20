package com.sproutloud.starter.stream.dao;

import java.util.Map;
import java.util.Set;

/**
 * Queries database
 *
 * @author rgupta
 */
public interface DedupeDao {

    /**
     * Queries database for existing records with same dedupe_hash
     *
     * @param db           database name
     * @param table        table name
     * @param dedupeHash
     * @param localityCode
     * @param listId
     * @param columnNames
     * @return Map of all the fields for the existing record in database
     */
    Map<String, String> getRecordFromListData(String db, String table, String dedupeHash, String localityCode,
                                              String listId, Set<String> columnNames);

}
