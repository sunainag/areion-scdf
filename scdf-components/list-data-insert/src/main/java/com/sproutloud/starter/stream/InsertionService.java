package com.sproutloud.starter.stream;

import com.sproutloud.starter.stream.dao.InsertionDao;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Provides connection to database with given connection details and dbName and tableName.
 * Triggers DML queries for {@link com.sproutloud.starter.stream.ListDataInsertApplication}
 */
@Slf4j
@Service
public class InsertionService {

    @Autowired
    InsertionDao dao;

    /**
     * Supports the persistence the input data to database table. Extracts the column name and values from the input
     * columnMap, and triggers and insert to the database table.
     *
     * @param dbName    Name of database to which data in columnMap is saved
     * @param tableName Database table to which data in columnMap is saved
     * @param columnMap Map with keys as column names and values as respective values to save to database table
     */
    public void persistToDb(String dbName, String tableName, Map<String, Object> columnMap) throws SQLException {
        prepareDbData(columnMap);
        String columnKeyString = String.join(",", columnMap.keySet());
        String columnValueString = columnMap.values().stream().map(val -> "'" + val + "'")
                .collect(Collectors.joining(","));
        log.debug(String.format("Inserting row %s to table %s %n", columnMap.keySet().stream()
                        .map(key -> key + "=" + columnMap.get(key)).collect(Collectors.joining(",", "{", "}")),
                dbName + "." + tableName));
        dao.insertRowData(dbName, tableName, columnKeyString, columnValueString);
    }

    /**
     * Formats or adds the necessary column data for database table
     *
     * @param columnMap Map with keys as column names and values as respective values to save to database table
     */
    private void prepareDbData(Map<String, Object> columnMap) throws SQLException {
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        columnMap.put("modified_ts", ts);
        columnMap.put("created_ts", ts);
        columnMap.put("user_created_ts", ts);

        String input = (String) columnMap.get("job_id");
        columnMap.put("job_id", dao.createArray(input.split(",")));

        input = (String) columnMap.get("recipient_source");
        columnMap.put("recipient_source", dao.createArray(input.split(",")));
    }
}
