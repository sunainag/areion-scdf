package com.sproutloud.starter.stream.dao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

/**
 * Provides JDBC support to connect to database. Check {@link DataSource} for details. Provide db properties for
 * application to connect to respective platform. The database platform depends on the properties mentioned for the
 * application, set for postgresql: spring.datasource.platform=postgresql
 * Maintains a cache for the frequently visited db queries
 */
@Repository
public class InsertionDao extends JdbcDaoSupport {

    @Autowired
    DataSource dataSource;

    @PostConstruct
    private void initialize() {
        setDataSource(dataSource);
    }

    /**
     * Inserts row data to given table
     *
     * @param dbName       Database name to run the insert query
     * @param tableName    Database table name to insert data
     * @param columnNames  comma separated column names for insert query
     * @param columnValues respective comma separated column values for insert query
     */
    @Transactional
    public void insertRowData(String dbName, String tableName, String columnNames, String columnValues) {
        StringBuffer insertListData = new StringBuffer().append("INSERT INTO ");
        if (!StringUtils.isEmpty(dbName)) {
            insertListData.append(dbName + ".");
        }
        insertListData.append(tableName).append(" (").append(columnNames).append(") values(").append(columnValues).append(")");
        Objects.requireNonNull(getJdbcTemplate()).update(insertListData.toString());
    }

    /**
     * Creates an array value in database, using the Array java object
     *
     * @param inputArr used to create sql array
     * @return the sql array created
     * @throws SQLException if fails to get Connection
     */
    public Array createArray(Object[] inputArr) throws SQLException {
        try (final Connection connection = dataSource.getConnection();) {
            return connection.createArrayOf("text", inputArr);
        }
    }
    
    public JdbcTemplate getJdbcTemplateForTest() {
        return Objects.requireNonNull(getJdbcTemplate());
    }
}
