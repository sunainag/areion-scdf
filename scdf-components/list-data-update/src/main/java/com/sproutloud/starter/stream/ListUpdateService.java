package com.sproutloud.starter.stream;

import static com.sproutloud.starter.stream.constants.ApplicationConstants.DATABASE;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.DB_FIELDS;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.FIELDS_DATA;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.LIST_ID;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.LOCALITY_CODE;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.MODIFIED_OP;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.RECIPIENT_ID;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.TABLE;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sproutloud.starter.stream.dao.impl.UpdateDaoImpl;

import lombok.extern.log4j.Log4j2;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

/**
 * Provides connection to database with given connection details and dbName and tableName. 
 * Creates key value pair for updating the details in database.
 */
@Log4j2
@Component
public class ListUpdateService {

    @Autowired
    private ObjectMapper mapper;

    @Autowired
    private UpdateDaoImpl dao;

    /**
     * Prepares the key value pair using the fields data and db fields.
     * If for a field, value is present in fields data it is taken else value from db fields is taken. 
     * For job_id and recipient_source a unique list is prepared combining both the lists from db fields and fields data.
     * 
     * @param input incoming json message
     * @throws SQLException when given set is not able to convert to Array.
     */
    public void updateList(Map<String, Object> input) throws SQLException {
        if (Objects.nonNull(input.get(TABLE))) {
            String tableName = (String) input.get(TABLE);
            String db = (String) input.get(DATABASE);
            Map<String, Object> fieldsData = mapper.convertValue(input.get(FIELDS_DATA), new TypeReference<Map<String, Object>>() {
            });
            Map<String, Object> dbData = mapper.convertValue(input.get(DB_FIELDS), new TypeReference<Map<String, Object>>() {
            });

            fieldsData.put(MODIFIED_OP, "U");
            List<String> setValues = new ArrayList<>();
            for (Entry<String, Object> entry : fieldsData.entrySet()) {
                String field = entry.getKey();
                if (RECIPIENT_ID.equals(field) || LIST_ID.equals(field) || LOCALITY_CODE.equals(field)) {
                    continue;
                }
                Object value = null;
                if ("job_id".equals(field) || "recipient_source".equals(field)) {
                    value = getUniqueList((String) fieldsData.get(field), dbData.get(field));
                } else {
                    value = (String) fieldsData.get(field);
                    if (StringUtils.isEmpty(value)) {
                        value = (String) dbData.get(field);
                    }
                }
                setValues.add(field + " = '" + value + "'");
            }
            Timestamp ts = new Timestamp(System.currentTimeMillis());
            setValues.add("modified_ts = '" + ts + "'");
            dao.updateListData(tableName, db, setValues, (String) fieldsData.get(LIST_ID), (String) dbData.get(RECIPIENT_ID));
        } else {
            log.error("Database and table name are missing in the input json");
        }
    }

    /**
     * unique list is prepared combining both the lists from db fields and fields data.
     * Convert unique list to array which can be inserted to DB.
     * 
     * @param fieldData String of value.
     * @param dbData    {@link Map} with key value pair of field name and field value present in db.
     * @return Array with unique set.
     * @throws SQLException when given set is not able to convert to Array.
     */
    private Object getUniqueList(String fieldData, Object dbData) throws SQLException {
        String dbDataString = String.valueOf(dbData);
        dbDataString = dbDataString.substring(1, dbDataString.length() - 1);
        dbDataString = dbDataString.replaceAll(" ", "");
        List<String> dbDataList = Arrays.asList(dbDataString.split(","));
        Set<String> uniqueSet = new HashSet<>();
        if (!StringUtils.isEmpty(fieldData)) {
            uniqueSet.add(fieldData);
        }
        if (!CollectionUtils.isEmpty(dbDataList)) {
            uniqueSet.addAll(dbDataList);
        }
        return dao.createArray(uniqueSet.toArray());
    }
}
