package com.sproutloud.starter.stream;

import com.sproutloud.starter.stream.dao.DedupeDao;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.Map;

/**
 * Performs the deduplication of incoming data
 *
 * @author rgupta
 */
@Slf4j
@Component
public class DedupeHandler {

    @Autowired
    private DedupeDao dao;

    /**
     * Takes the input data and return the output data after setting router_flag and
     * existing db fields
     *
     * @param fieldData Map of the fields' data
     * @param db        database name
     * @param table     table name
     * @return Map<String, String>
     */
    public Map<String, String> execute(Map<String, Object> fieldData, String db, String table) {
        log.debug("Checking for existing entry in database");
        return dao.getRecordFromListData(db, table, String.valueOf(fieldData.get("dedupe_hash")), String.valueOf(fieldData.get("locality_code")),
                String.valueOf(fieldData.get("list_id")), fieldData.keySet());
    }

    /**
     * Check if all the required fields are present in the incoming input JSON
     *
     * @param fieldData Map of the fields' data
     * @param db        database name
     * @param table     table name
     * @return String
     */
    public String isValid(Map<String, Object> fieldData, String db, String table) {
        if (StringUtils.isEmpty(db)) {
            return "Invalid Database";
        } else if (StringUtils.isEmpty(table)) {
            return "Invalid Target table name";
        } else if (StringUtils.isEmpty(fieldData.get("dedupe_hash"))) {
            return "Invalid dedupe_hash value";
        } else if (StringUtils.isEmpty(fieldData.get("locality_code"))) {
            return "Invalid locality_code value";
        } else if (StringUtils.isEmpty(fieldData.get("list_id"))) {
            return "Invalid list_id value";
        } else {
            return "valid";
        }
    }
}
