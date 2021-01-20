package com.sproutloud.starter.stream.dao;

import java.util.List;
import java.util.Map;

/**
 * Used for communication to database for inserting job status data
 * 
 * @author rgupta
 *
 */
public interface TaskAggregatorDao {

    
    /**
     * Runs update statement for job status.
     * 
     * @param jobId parent_job_id of the job to be updated.
     * @param jobType job_type to be updated.
     * @param time job complete time.
     */
    void writeStatusToDb(String jobId, String jobType, Long time);

    /**
     * Retrives the child jobs available for given jobId.
     * 
     * @param jobId id for which child jobs are retrieved.
     * @return {@link List} of child jobs.
     */
    List<Map<String, Object>> getChildJobs(String jobId);

    /**
     * Runs update statement for job status.
     * 
     * @param jobId
     * @param string
     */
    void writeFailedStatusToDb(String jobId, Long time);

    /**
     * Runs update statement for parent job status.
     * 
     * @param jobId parent_job_id of the job to be updated.
     * @param jobType job_type to be updated.
     * @param time job complete time.
     * @param stats 
     */
    void writeParentStatusToDb(String jobId, String jobType, Long time, String stats);

    /**
     * marks the status of the jobs to completed.
     * 
     * @param jobId           parent_job_id of the jobs to be updated.
     * @param jobsToBeUpdated {@link List} of child jobs o be updated.
     * @param time            modified time
     */
    void updateJobStatusToDb(String jobId, List<String> jobsToBeUpdated, Long time);

}
