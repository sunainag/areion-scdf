package com.sproutloud.starter.stream.dao.impl;

import com.sproutloud.starter.stream.dao.UpdateDao;

import lombok.extern.log4j.Log4j2;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.stereotype.Repository;
import org.springframework.util.StringUtils;

import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

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
public class UpdateDaoImpl extends JdbcDaoSupport implements UpdateDao {

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
     * Creates an array value in database, using the Array java object
     *
     * @param inputArr used to create sql array
     * @return the sql array created
     * @throws SQLException if fails to get Connection
     */
    @Override
    public Array createArray(Object[] inputArr) throws SQLException {
        try (final Connection connection = dataSource.getConnection();) {
            return connection.createArrayOf("text", inputArr);
        }
    }

    /**
     * Updates the entry into the database.
     * 
     * @param tableName   table name into which data has to be updated
     * @param dbName      database name into which data has to be updated
     * @param setValues   values to be updated
     * @param listId      list id of the entry to be updated
     * @param recipientId recipient id of the entry to be updated
     */
    @Override
    public void updateListData(String tableName, String dbName, List<String> setValues, String listId, String recipientId) {
        String setValuesString = String.join(",", setValues);
        StringBuffer updateListData = new StringBuffer().append("UPDATE ");
        if (!StringUtils.isEmpty(dbName)) {
            updateListData.append(dbName + ".");
        }
        updateListData.append(tableName).append(" SET ").append(setValuesString).append(" WHERE locality_code = 'SL_US' AND list_id = '")
                .append(listId).append("'  AND recipient_id = '").append(recipientId).append("'");
        try {
            Objects.requireNonNull(getJdbcTemplate()).update(updateListData.toString());
            log.debug("Updated List Data Successfully.");
        } catch (Exception e) {
            log.error("Exception while updating data to DB.");
        }

    }
}
