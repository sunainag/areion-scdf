package com.sproutloud.starter.stream.dao;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Performs the DB operations.
 * 
 * @author mgande
 *
 */
public interface FieldsDao {

    /**
     * Fetches the field details of the fieldNames provided and mapped to the given list_id
     * 
     * @param listId     String of the list_id
     * @param fieldNames {@link Set} of fields for which details have to be fetched.
     * @return {@link List} of {@link Map} with key as field detail name and value as field detail value.
     */
    public List<Map<String, String>> getListFieldsData(String listId, Set<String> fieldNames);

    /**
     * Fetches the sequence number of the sequence given and increments it by given size in db and returns earlier value.
     * 
     * @param size     by which sequence number has to be increased.
     * @param sequence for which sequence number has to be fetched and updated.
     * @return {@link Integer} sequence number obtained before updating.
     */
    public Integer getSequenceNumber(Integer size, String sequence);

    /**
     * Fetches the segment details mapped to given list_id.
     * 
     * @param listId for which segment details have to be fetched.
     * @return {@link Map} of segment details.
     */
    public Map<String, String> getSegmentDetails(String listId);

    /**
     * Inserts the data to acct_members table.
     * 
     * @param tpAccountsInsertValues String comprising the values to be inserted.
     */
    public void insertTpAccountDetails(List<String> tpAccountsInsertValues);

    /**
     * Inserts the data to list_meta table.
     * 
     * @param segmentInsertValues String comprising the values to be inserted.
     */
    public void insertSegmentDetails(List<String> segmentInsertValues);

    /**
     * Updates the status in jobs table to completed.
     * 
     * @param jobId for which status has to be updated
     * @param time updated time stamp
     */
    void updateJobStatus(Object jobId, Long time);

}
