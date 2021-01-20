package com.sproutloud.starter.stream.dao.impl;

import java.util.List;
import java.util.Objects;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.stereotype.Repository;

import com.sproutloud.starter.stream.dao.LeaseDao;

import lombok.extern.log4j.Log4j2;

/**
 * Class that implements LeaseDao interface
 * 
 * @author rgupta
 *
 */
@Log4j2
@Repository
public class LeaseDaoImpl extends JdbcDaoSupport implements LeaseDao {

    /**
     * DataSource object to be used
     */
    @Autowired
    private DataSource dataSource;

    /**
     * Initialises the dao implementation and sets the data source to DataSource
     * object created
     */
    @PostConstruct
    private void initialize() {
        setDataSource(dataSource);
    }

    /**
     * Runs insert statement for lease data
     * 
     * @param database
     * @param accountId
     * @param columnKeys
     * @param columnValues
     */
    @Override
    public void runLeaseInsert(String database, String accountId, String columnKeys, List<String> columnValues) {
        String tableName = database + "." + "lease_data_" + accountId.toLowerCase();
        String insertQuery = "INSERT INTO " + tableName + " (" + columnKeys + ") VALUES ";

        for (String columnVal : columnValues) {
            insertQuery += "(" + columnVal + "),";
        }

        insertQuery = insertQuery.substring(0, insertQuery.length() - 1);

        insertQuery += " ON CONFLICT DO NOTHING";
        try {
            Objects.requireNonNull(getJdbcTemplate()).update(insertQuery);
            log.info("Inserted lease data to DB successfully.");
        } catch (Exception e) {
            log.error("Error while inserting lease data to DB" + e.getMessage());
        }

    }
}
