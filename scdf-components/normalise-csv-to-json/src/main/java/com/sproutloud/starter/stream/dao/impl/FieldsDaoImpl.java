package com.sproutloud.starter.stream.dao.impl;

import com.sproutloud.starter.stream.dao.FieldsDao;

import lombok.extern.log4j.Log4j2;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

/**
 * Performs the Db operations.
 * 
 * @author mgande
 *
 */
@Log4j2
@Repository
public class FieldsDaoImpl extends JdbcDaoSupport implements FieldsDao {

    /**
     * {@link DataSource} bean.
     */
    @Autowired
    DataSource dataSource;

    @PostConstruct
    private void initialize() {
        setDataSource(dataSource);
    }

    /**
     * Fetches the segment details mapped to given list_id.
     * 
     * @param listId for which segment details have to be fetched.
     * @return {@link Map} of segment details.
     */
    @Override
    public Map<String, String> getSegmentDetails(String listId) {
        String query = "select tp_list_id , list_id from list_meta where master_list_id = '" + listId
                + "' and is_active =true and tp_list_id is not null and locality_code ='SL_US'";
        return Objects.requireNonNull(getJdbcTemplate()).query(query, new ResultSetExtractor<Map<String, String>>() {
            @Override
            public Map<String, String> extractData(ResultSet rs) throws SQLException, DataAccessException {
                HashMap<String, String> mapRet = new HashMap<String, String>();
                while (rs.next()) {
                    mapRet.put(rs.getString("tp_list_id"), rs.getString("list_id"));
                }
                return mapRet;
            }
        });
    }

    /**
     * Fetches the field details of the fieldNames provided and mapped to the given list_id
     * 
     * @param listId     String of the list_id
     * @param fieldNames {@link Set} of fields for which details have to be fetched.
     * @return {@link List} of {@link Map} with key as field detail name and value as field detail value.
     */
    @Override
    @Cacheable(value = "listFields")
    public List<Map<String, String>> getListFieldsData(String listId, Set<String> fieldNames) {
        fieldNames = fieldNames.stream().map(name -> ("'" + name + "'")).collect(Collectors.toSet());
        String fieldNamesString = String.join(",", fieldNames);
        String fieldsData = "select field_name, data_type, field_type, field_format, is_required from list_fields where list_id = '" + listId
                + "' and is_active = true and (field_name in (" + fieldNamesString + " ) or is_required = true)";
        String[] columnNames = { "field_name", "data_type", "field_type", "field_format", "is_required" };
        return Objects.requireNonNull(getJdbcTemplate()).query(fieldsData, getExtractor(columnNames));
    }

    /**
     * Creates and returns the mapper for fetching field details.
     * 
     * @param columnNames field names to prepare the {@link Map}
     * @return {@link RowMapper} to extract filed details.
     */
    private RowMapper<Map<String, String>> getExtractor(String[] columnNames) {
        RowMapper<Map<String, String>> extractor = (rs, rowNum) -> {
            Map<String, String> resultMap = new HashMap<>();
            for (String column : columnNames) {
                resultMap.put(column, rs.getString(column));
            }
            return resultMap;
        };
        return extractor;
    }

    /**
     * Fetches the sequence number of the sequence given and increments it by given size in db and returns earlier value.
     * 
     * @param size     by which sequence number has to be increased.
     * @param sequence for which sequence number has to be fetched and updated.
     * @return {@link Integer} sequence number obtained before updating.
     */
    @Override
    public Integer getSequenceNumber(Integer size, String sequence) {
        Integer num = Objects.requireNonNull(getJdbcTemplate()).queryForObject("select nextval('" + sequence + "')", Integer.class);
        Objects.requireNonNull(getJdbcTemplate()).execute("alter sequence " + sequence + " increment " + size);
        return num;
    }

    /**
     * Inserts the data to list_meta table.
     * 
     * @param segmentInsertValues String comprising the values to be inserted.
     */
    @Override
    public void insertSegmentDetails(List<String> segmentInsertValues) {
        String insertValues = String.join(",", segmentInsertValues);
        String query = "insert into list_meta (modified_ts , modified_by , modified_op , created_by , created_ts , list_id , "
                + "locality_code , account_id , master_list_id , list_name, list_type, tp_list_id) values " + insertValues;
        try {
            Objects.requireNonNull(getJdbcTemplate()).update(query);
            log.info("Inserted segment data to DB successfully.");
        } catch (Exception e) {
            log.error("Error while inserting segment data to DB" + e.getMessage());
        }

    }

    /**
     * Inserts the data to acct_members table.
     * 
     * @param tpAccountsInsertValues String comprising the values to be inserted.
     */
    @Override
    public void insertTpAccountDetails(List<String> tpAccountsInsertValues) {
        String insertValues = String.join(",", tpAccountsInsertValues);
        String query = "insert into acct_leasing_keys (modified_ts , modified_by , modified_op , created_by , created_ts , "
                + "account_id , locality_code , tp_id ) values " + insertValues + "on conflict do nothing";
        try {
            Objects.requireNonNull(getJdbcTemplate()).update(query);
            log.info("Inserted tp account data to DB successfully.");
        } catch (Exception e) {
            log.error("Error while inserting segment data to DB" + e.getMessage());
        }
    }

    /**
     * Updates the status in jobs table to completed.
     * 
     * @param jobId for which status has to be updated
     * @param time  updated time stamp
     */
    @Override
    public void updateJobStatus(Object jobId, Long time) {
        Timestamp ts = new Timestamp(time);
        String query = "update jobs set job_status = 'completed', modified_ts = '" + ts + "' where parent_job_id = '" + jobId
                + "' and job_type = 'csv_normalization'";
        Objects.requireNonNull(getJdbcTemplate()).execute(query);
    }
}
