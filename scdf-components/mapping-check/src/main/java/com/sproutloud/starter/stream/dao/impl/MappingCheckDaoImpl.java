package com.sproutloud.starter.stream.dao.impl;

import com.sproutloud.starter.stream.dao.MappingCheckDao;

import lombok.extern.log4j.Log4j2;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.sql.DataSource;

/**
 * Provides JDBC support to connect to database. Check {@link DataSource} for details. Provide db properties for application to connect to respective
 * platform. The database platform depends on the properties mentioned for the application, set for postgresql: spring.datasource.platform=postgresql
 * Maintains a cache for the frequently visited db queries
 * 
 * @author mgande
 *
 */
@Log4j2
@Repository
public class MappingCheckDaoImpl implements MappingCheckDao {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * Checks if an entry is present in integrations with given details.
     * 
     * @param integrationId integration_id for the check.
     * @param listId       list_id for the check.
     * @param accountId    account_id for the check.
     * @return {@link Map} of the integrations.
     */
    @Override
    public Map<String, Object> getIntegrations(String integrationId, String listId, String accountId) {
        String query = "select integration_id from integrations where integration_id  = '" + integrationId + "' and list_id = '" + listId
                + "' and account_id = '" + accountId + "' and is_active = 'true'";
        try {
            return Objects.requireNonNull(jdbcTemplate).queryForMap(query);
        } catch (Exception e) {
            log.error("Unable to get Integration details with given details.");
            throw new RuntimeException("Unable to get Integration with given details.");
        }
    }

    /**
     * Fetches the integration mappings for the given integration id.
     * 
     * @param integrationId id to fetch integration mappings.
     * @return {@link List} of integration mappings.
     */
    @Override
    public List<Map<String, Object>> getIntegrationMappings(String integrationId) {
        String query = "select mapping_type, source_field, target_field from integration_mappings am where integration_id  = '" + integrationId + "'";
        try {
            return Objects.requireNonNull(jdbcTemplate).queryForList(query);
        } catch (Exception e) {
            log.error("Integration Mappings with given integration id are not available.");
            throw new RuntimeException("Integration Mappings with given integration id are not available.");
        }
    }

    /**
     * Updates the status in jobs table to completed.
     * 
     * @param jobId for which status has to be updated
     * @param time updated time stamp
     */
    @Override
    public void updateJobStatus(Object jobId, Long time) {
        Timestamp ts = new Timestamp(time);
        String query = "update jobs set job_status = 'completed', modified_ts = '" + ts + "' where parent_job_id = '" + jobId
                + "' and job_type = 'mapping_check'";
        Objects.requireNonNull(jdbcTemplate).execute(query);
    }
}
