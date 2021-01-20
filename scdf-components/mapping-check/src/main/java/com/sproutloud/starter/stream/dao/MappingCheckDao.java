package com.sproutloud.starter.stream.dao;

import java.util.List;
import java.util.Map;

/**
 * Database interactions are handled.
 * 
 * @author mgande
 *
 */
public interface MappingCheckDao {

    /**
     * Checks if an entry is present in integrations with given details.
     * 
     * @param integrationId integration_id for the check.
     * @param listId       list_id for the check.
     * @param accountId    account_id for the check.
     * @return {@link Map} of the integrations.
     */
    public Map<String, Object> getIntegrations(String integrationId, String listId, String accountId);

    /**
     * Fetches the integration mappings for the given integration id.
     * 
     * @param integrationId id to fetch integration mappings.
     * @return {@link List} of integration mappings.
     */
    public List<Map<String, Object>> getIntegrationMappings(String integrationId);

    /**
     * Updates the status in jobs table to completed.
     * 
     * @param jobId for which status has to be updated
     * @param time updated time stamp
     */
    void updateJobStatus(Object jobId, Long time);
}
