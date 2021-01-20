package com.sproutloud.starter.stream.dao.impl;

import com.sproutloud.starter.stream.dao.TaskAggregatorDao;

import lombok.extern.log4j.Log4j2;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

/**
 * Class that implements LeaseDao interface
 * 
 * @author rgupta
 *
 */
@Log4j2
@Repository
public class TaskAggregatorDaoImpl extends JdbcDaoSupport implements TaskAggregatorDao {

    /**
     * DataSource object to be used
     */
    @Autowired
    private DataSource dataSource;

    /**
     * Initialises the dao implementation and sets the data source to DataSource object created
     */
    @PostConstruct
    private void initialize() {
        setDataSource(dataSource);
    }

    /**
     * Runs update statement for job status.
     * 
     * @param jobId   parent_job_id of the job to be updated.
     * @param jobType job_type to be updated.
     * @param time    job complete time.
     */
    @Override
    public void writeStatusToDb(String jobId, String jobType, Long time) {
        log.debug("updating status in the database");
        Timestamp ts = new Timestamp(time);
        String query = "update jobs set job_status = 'completed', modified_ts = '" + ts + "' where parent_job_id = '" + jobId + "' and job_type = '"
                + jobType + "'";
        Objects.requireNonNull(getJdbcTemplate()).execute(query);
    }

    /**
     * Retrives the child jobs available for given jobId.
     * 
     * @param jobId id for which child jobs are retrieved.
     * @return {@link List} of child jobs.
     */
    @Override
    public List<Map<String, Object>> getChildJobs(String jobId) {
        Set<String> columnNames = new HashSet<String>();
        columnNames.add("job_type");
        columnNames.add("order_number");
        columnNames.add("parent_job_type");
        String query = "SELECT lrj.job_type, lrj.order_number, lrj.parent_job_type FROM lookup_rel_job_types lrj WHERE lrj.parent_job_type = (SELECT job_type FROM jobs j WHERE j.job_id = '"
                + jobId + "')";
        List<Map<String, Object>> resultSet = Objects.requireNonNull(getJdbcTemplate()).queryForList(query);
        if (!CollectionUtils.isEmpty(resultSet)) {
            log.debug("Response from database: " + resultSet);
            return resultSet;
        }
        return new ArrayList<>();
    }

    /**
     * Runs update statement for job status.
     * 
     * @param jobId
     * @param string
     */
    @Override
    public void writeFailedStatusToDb(String jobId, Long time) {
        Timestamp ts = new Timestamp(time);
        String query = "update jobs set job_status = 'completed', modified_ts = '" + ts + "' where job_id = '" + jobId + "' or parent_job_id = '"
                + jobId + "'";
        Objects.requireNonNull(getJdbcTemplate()).execute(query);
    }

    /**
     * Runs update statement for parent job status.
     * 
     * @param jobId   parent_job_id of the job to be updated.
     * @param jobType job_type to be updated.
     * @param time    job complete time.
     */
    @Override
    public void writeParentStatusToDb(String jobId, String jobType, Long time, String stats) {
        Timestamp ts = new Timestamp(time);
        String query = "update jobs set job_status = 'completed', modified_ts = '" + ts + "', stats = '" + stats + "' where job_id = '" + jobId
                + "' and job_type = '" + jobType + "'";
        Objects.requireNonNull(getJdbcTemplate()).execute(query);
    }

    @Override
    public void updateJobStatusToDb(String jobId, List<String> jobsToBeUpdated, Long time) {
        Timestamp ts = new Timestamp(time);
        String jobTypes = jobsToBeUpdated.stream().map(job -> "'" + job + "'").collect(Collectors.joining(","));
        String query = "update jobs set job_status = 'completed', modified_ts = '" + ts + "' where parent_job_id = '" + jobId + "' and job_type in ("
                + jobTypes + ")";
        Objects.requireNonNull(getJdbcTemplate()).execute(query);
    }
}