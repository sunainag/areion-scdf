package com.sproutloud.starter.stream.dao;

import java.sql.Array;
import java.sql.SQLException;
import java.util.List;

/**
 * Performs the DB operations.
 * 
 * @author mgande
 *
 */
public interface UpdateDao {

    /**
     * Creates an array value in database, using the Array java object
     *
     * @param inputArr used to create sql array
     * @return the sql array created
     * @throws SQLException if fails to get Connection
     */
    public Array createArray(Object[] inputArr) throws SQLException;

    /**
     * Updates the entry into the database.
     * 
     * @param tableName   table name into which data has to be updated
     * @param dbName      database name into which data has to be updated
     * @param setValues   values to be updated
     * @param listId      list id of the entry to be updated
     * @param recipientId recipient id of the entry to be updated
     */
    public void updateListData(String tableName, String dbName, List<String> setValues, String listId, String recipientId);
}
