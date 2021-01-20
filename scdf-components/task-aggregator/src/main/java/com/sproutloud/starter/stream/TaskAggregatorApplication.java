package com.sproutloud.starter.stream;

import static com.sproutloud.starter.stream.properties.TaskAggregatorProperties.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sproutloud.starter.stream.dao.TaskAggregatorDao;
import com.sproutloud.starter.stream.redis.RedisObjectService;

import lombok.extern.log4j.Log4j2;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.messaging.Message;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Keeps track of progress of ingestion jobs
 * 
 * @author rgupta
 *
 */
@Log4j2
@EnableBinding(Sink.class)
@SpringBootApplication
public class TaskAggregatorApplication {

    /**
     * mapper object to be used
     */
    @Autowired
    private ObjectMapper mapper;

    @Autowired
    private RedisObjectService redisObject;

    /**
     * Object of LeaseDao class to call methods that communicate with database
     */
    @Autowired
    private TaskAggregatorDao dao;

    /**
     * Triggers the simple spring boot application
     * 
     * @param args
     */
    public static void main(String[] args) {
        SpringApplication.run(TaskAggregatorApplication.class, args);
    }

    /**
     * formats the incoming data to track progress
     * 
     * @param message
     * @throws IOException
     */
    @StreamListener(Sink.INPUT)
    public void processInputRequest(Message<?> message) throws IOException {
        JsonNode jsonNode = mapper.readTree((String) message.getPayload());
        Map<String, Object> inputMsg = mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
        });
        String validityStatus = validityCheck(inputMsg);
        if (validityStatus == "valid") {
            log.debug("********* Checking the input message ******************* \n" + inputMsg);
            if (inputMsg.get(JOB_TYPE).toString().contains("to_")) {
                return;
            }
            updateProgress(inputMsg);
        } else {
            log.error("Input data is invalid: " + validityStatus);
        }
    }

    /**
     * Checks if all the required fields are present in the input.
     * 
     * @param inputMsg {@link Map} to validate.
     * @return String returns Valid if success.
     */
    public String validityCheck(Map<String, Object> inputMsg) {
        if (StringUtils.isEmpty(inputMsg.get(JOB_TYPE))) {
            return "Invalid job type";
        }

        Map<String, Object> fieldsData = mapper.convertValue(inputMsg.get("fields_data"),
                new TypeReference<Map<String, Object>>() {
                });
        if (StringUtils.isEmpty(inputMsg.get(JOB_ID))
                && (CollectionUtils.isEmpty(fieldsData) || StringUtils.isEmpty(fieldsData.get(JOB_ID)))) {
            return "Invalid job id";
        }
        String jobId = (String) inputMsg.get(JOB_ID);
        if (StringUtils.isEmpty(jobId)) {
            jobId = (String) fieldsData.get(JOB_ID);
        }
        inputMsg.put(JOB_ID, jobId);
        return "valid";
    }

    /**
     * formats the output data to be sent to sink
     * 
     * @param inputMsg {@link Map} update progress.
     * @throws JsonProcessingException
     */
    public void updateProgress(Map<String, Object> inputMsg) throws JsonProcessingException {
        log.info("fetching job_id and job_type from the input message: " + inputMsg);
        String jobId = (String) inputMsg.get(JOB_ID);
        String jobType = (String) inputMsg.get(JOB_TYPE);

        if ("csv_normalization".equals(jobType)) {
            jobType = VALIDATION_FAILED;
        }
        log.debug("Checking if the job is already in the json");
        if (Objects.isNull(redisObject.getByJobId(jobId))) {
            synchronized (jobId) {
                if (Objects.isNull(redisObject.getByJobId(jobId))) {
                    createaNewRedisEntry(inputMsg, jobId);
                }
            }
        }

        String key = jobId + jobType;
        key = key.intern();
        synchronized (key) {
            updateRedisEntry(inputMsg, jobId, jobType);
        }
    }

    /**
     * Updates the Redis entry with appropriate data using job_type and job_id.
     * Updates DB job_status as complete if all the entries of particular job_type
     * of a job_id is done.
     * 
     * @param inputMsg {@link Map} to get details of the process.
     * @param jobId    id of the job to be updated.
     * @param jobType  type of the job to be updated.
     * @throws JsonProcessingException
     */
    private void updateRedisEntry(Map<String, Object> inputMsg, String jobId, String jobType)
            throws JsonProcessingException {
        log.debug("Updating the existing entry.");
        Map<String, Object> jobStatus = mapper.convertValue(redisObject.getByJobId(jobId),
                new TypeReference<Map<String, Object>>() {
                });
        if (CollectionUtils.isEmpty(jobStatus)) {
            return;
        }
        log.info("************************JobStatus is*******************\n" + jobStatus);
        Integer numRecords = Integer.parseInt(jobStatus.get(NUMBER_OF_RECORDS).toString());
        if (VALIDATION_FAILED.equals(jobType)) {
            numRecords--;
            if (numRecords == 0) {
                dao.writeFailedStatusToDb(jobId,
                        (Long) inputMsg.get(com.sproutloud.starter.stream.StringUtils.OUT_TIME));
                redisObject.remove(jobId);
                return;
            }
            jobStatus.put(NUMBER_OF_RECORDS, numRecords);
        }

        if ("dedupe".equals(jobType)) {
            if (inputMsg.get("router_flag").equals("INSERT")) {
                Integer count = Integer.parseInt(jobStatus.get(RECIPIENT_INSERT_COUNT).toString()) + 1;
                jobStatus.put(RECIPIENT_INSERT_COUNT, count);
            } else {
                Integer count = Integer.parseInt(jobStatus.get(RECIPIENT_UPDATE_COUNT).toString()) + 1;
                jobStatus.put(RECIPIENT_UPDATE_COUNT, count);
            }
        }

        String countKey = jobType + "_count";
        String reqKey = jobType + "_req_count";
        String doneKey = jobType + "_done";
        log.debug("incrementing the count for the appropriate job type");
        /*
         * if (StringUtils.isEmpty(jobStatus.get(countKey))) {
         * redisObject.remove(jobId); return; }
         */
        log.info("countkey is" + countKey);

        Integer countVal = Integer.parseInt(jobStatus.get(countKey).toString()) + 1;

        jobStatus.put(countKey, countVal);
        jobStatus.put(JOB_MODIFIED_TIMESTAMP, inputMsg.get(com.sproutloud.starter.stream.StringUtils.OUT_TIME));

        log.debug("checking if all the records have been processed");
        boolean isJobDone = false;
        if (countVal.equals(numRecords) && !VALIDATION_FAILED.equals(jobType)) {
            if (VALIDATION_SUCCESS.equals(jobType)) {
                jobStatus.put(DATA_VALIDATION_DONE, true);
                dao.writeStatusToDb(jobId, "data_validation", (Long) jobStatus.get(JOB_MODIFIED_TIMESTAMP));
            } else {
                jobStatus.put(doneKey, true);
                dao.writeStatusToDb(jobId, jobType, (Long) jobStatus.get(JOB_MODIFIED_TIMESTAMP));
            }
            isJobDone = true;
        } else if (jobType.contains("lease") || jobType.contains("segment")) {
            if (!Objects.isNull(jobStatus.get(reqKey)) && Objects.equals(jobStatus.get(reqKey), jobStatus.get(countKey))
                    && (boolean) jobStatus.get(DATA_INGESTION_DONE)) {
                jobStatus.put(doneKey, true);
                isJobDone = true;
                dao.writeStatusToDb(jobId, jobType, (Long) jobStatus.get(JOB_MODIFIED_TIMESTAMP));
            }
        } else if (!Objects.isNull(jobStatus.get(reqKey))
                && Objects.equals(jobStatus.get(reqKey), jobStatus.get(countKey))
                && (boolean) jobStatus.get(DATA_VALIDATION_DONE)) {
            jobStatus.put(doneKey, true);
            isJobDone = true;
            dao.writeStatusToDb(jobId, jobType, (Long) jobStatus.get(JOB_MODIFIED_TIMESTAMP));
        }

        log.debug(
                "If last record goes to failure channel then making data_validation and next steps as done if all previous rows are done.");
        if (VALIDATION_FAILED.equals(jobType) && numRecords.equals(jobStatus.get(VALIDATION_SUCCESS + "_count"))) {
            isJobDone = updateChildJobsForValidationFailure(jobId, jobStatus, numRecords, isJobDone);
        }

        log.debug("getting the count of records sent to each certification components");
        if (VALIDATION_SUCCESS.equals(jobType)) {
            updateValidationDependantCounts(inputMsg, jobId, jobStatus);
        }

        log.debug("getting the count of records sent to each ingestion components");
        if (DATA_INGESTION.equals(jobType)) {
            updateIngestionDependantCounts(inputMsg, jobId, jobStatus);
        }

        redisObject.save(jobId, jobStatus);

        if (isJobDone) {
            List<String> childJobs = (List<String>) jobStatus.get(CHILD_JOB_LIST);
            for (String childJob : childJobs) {
                if (!(boolean) jobStatus.get(childJob + "_done")) {
                    return;
                }
            }
            Map<String, Integer> statValue = new HashMap<>();
            statValue.put("total_records",
                    Integer.parseInt(jobStatus.get(VALIDATION_SUCCESS + "_count").toString())
                            + Integer.parseInt(jobStatus.get(VALIDATION_FAILED + "_count").toString())
                            + Integer.parseInt(jobStatus.get(DUPLICATE_RECORDS).toString()));
            statValue.put("duplicate_records", Integer.parseInt(jobStatus.get(DUPLICATE_RECORDS).toString()));
            statValue.put("accepted_records",
                    Integer.parseInt(jobStatus.get(VALIDATION_SUCCESS + "_count").toString()));
            statValue.put("rejected_records", Integer.parseInt(jobStatus.get(VALIDATION_FAILED + "_count").toString()));
            statValue.put("inserted_records", Integer.parseInt(jobStatus.get(RECIPIENT_INSERT_COUNT).toString()));
            statValue.put("updated_records", Integer.parseInt(jobStatus.get(RECIPIENT_UPDATE_COUNT).toString()));
            statValue.put("email_certified_records",
                    Integer.parseInt(jobStatus.get(EMAIL_CERTIFICATION_COUNT).toString()));
            statValue.put("phone_certified_records",
                    Integer.parseInt(jobStatus.get(PHONE_CERTIFICATION_COUNT).toString()));
            statValue.put("address_certified_records",
                    Integer.parseInt(jobStatus.get(ADDRESS_CERTIFICATION_COUNT).toString()));
            String stats = mapper.writeValueAsString(statValue);
            dao.writeParentStatusToDb(jobId, jobStatus.get(PARENT_JOB_TYPE).toString(),
                    (Long) jobStatus.get(JOB_MODIFIED_TIMESTAMP), stats);
            redisObject.remove(jobId);
        }
    }

    /**
     * Updates the required count of lease and segment ingestion components. Updates
     * the status if all the records are ingested.
     * 
     * @param inputMsg  {@link Map} to get details of the process.
     * @param jobId     id of the present job.
     * @param jobStatus {@link Map} of details of present jobId.
     */
    private void updateIngestionDependantCounts(Map<String, Object> inputMsg, String jobId,
            Map<String, Object> jobStatus) {
        if ((boolean) inputMsg.get(LEASE_DATA_INGESTION_REQ)) {
            Integer count = Integer.parseInt(jobStatus.get(LEASE_DATA_INGESTION_REQ_COUNT).toString()) + 1;
            jobStatus.put(LEASE_DATA_INGESTION_REQ_COUNT, count);
        }
        if ((boolean) inputMsg.get(SEGMENT_DATA_INGESTION_REQ)) {
            Integer count = Integer.parseInt(jobStatus.get(SEGMENT_DATA_INGESTION_REQ_COUNT).toString()) + 1;
            jobStatus.put(SEGMENT_DATA_INGESTION_REQ_COUNT, count);
        }
        if ((boolean) jobStatus.get(DATA_INGESTION_DONE)) {
            List<String> jobIds = new ArrayList<>();
            if (jobStatus.get(LEASE_DATA_INGESTION_REQ_COUNT).equals(jobStatus.get(LEASE_DATA_INGESTION + "_count"))) {
                jobStatus.put(LEASE_DATA_INGESTION + "_done", true);
                jobIds.add(LEASE_DATA_INGESTION);
            }
            if (jobStatus.get(SEGMENT_DATA_INGESTION_REQ_COUNT)
                    .equals(jobStatus.get(SEGMENT_DATA_INGESTION + "_count"))) {
                jobStatus.put(SEGMENT_DATA_INGESTION + "_done", true);
                jobIds.add(SEGMENT_DATA_INGESTION);
            }
            if (!jobIds.isEmpty()) {
                dao.updateJobStatusToDb(jobId, jobIds, (Long) jobStatus.get(JOB_MODIFIED_TIMESTAMP));
            }
        }
    }

    /**
     * Updates the required counts of all the certifications. Updates the status if
     * all the records are processed.
     * 
     * @param inputMsg  {@link Map} to get details of the process.
     * @param jobId     id of the present job.
     * @param jobStatus {@link Map} of details of present jobId.
     */
    private void updateValidationDependantCounts(Map<String, Object> inputMsg, String jobId,
            Map<String, Object> jobStatus) {
        if (com.sproutloud.starter.stream.StringUtils.IS_CERTIFICATION_REQUIRED
                .equals(inputMsg.get(com.sproutloud.starter.stream.StringUtils.EMAIL_CERTIFICATION))) {
            Integer count = Integer.parseInt(jobStatus.get(EMAIL_CERTIFICATION_REQ_COUNT).toString()) + 1;
            jobStatus.put(EMAIL_CERTIFICATION_REQ_COUNT, count);
        }
        if (com.sproutloud.starter.stream.StringUtils.IS_CERTIFICATION_REQUIRED
                .equals(inputMsg.get(com.sproutloud.starter.stream.StringUtils.PHONE_CERTIFICATION))) {
            Integer count = Integer.parseInt(jobStatus.get(PHONE_CERTIFICATION_REQ_COUNT).toString()) + 1;
            jobStatus.put(PHONE_CERTIFICATION_REQ_COUNT, count);
        }
        if (com.sproutloud.starter.stream.StringUtils.IS_CERTIFICATION_REQUIRED
                .equals(inputMsg.get(com.sproutloud.starter.stream.StringUtils.ADDRESS_CERTIFICATION))) {
            Integer count = Integer.parseInt(jobStatus.get(ADDRESS_CERTIFICATION_REQ_COUNT).toString()) + 1;
            jobStatus.put(ADDRESS_CERTIFICATION_REQ_COUNT, count);
        }

        if ((boolean) jobStatus.get(DATA_VALIDATION_DONE)) {
            List<String> jobIds = new ArrayList<>();
            if (jobStatus.get(ADDRESS_CERTIFICATION_REQ_COUNT)
                    .equals(jobStatus.get(ADDRESS_CERTIFICATION + "_count"))) {
                jobStatus.put(ADDRESS_CERTIFICATION + "_done", true);
                jobIds.add(ADDRESS_CERTIFICATION);
            }
            if (jobStatus.get(PHONE_CERTIFICATION_REQ_COUNT).equals(jobStatus.get(PHONE_CERTIFICATION + "_count"))) {
                jobStatus.put(PHONE_CERTIFICATION + "_done", true);
                jobIds.add(PHONE_CERTIFICATION);
            }
            if (jobStatus.get(EMAIL_CERTIFICATION_REQ_COUNT).equals(jobStatus.get(EMAIL_CERTIFICATION + "_count"))) {
                jobStatus.put(EMAIL_CERTIFICATION + "_done", true);
                jobIds.add(EMAIL_CERTIFICATION);
            }
            if (!jobIds.isEmpty()) {
                dao.updateJobStatusToDb(jobId, jobIds, (Long) jobStatus.get(JOB_MODIFIED_TIMESTAMP));
            }
        }
    }

    /**
     * Updates the status of the jobs if the last recors is validation failed.
     * 
     * @param jobId      id of the present job.
     * @param jobStatus  {@link Map} of details of present jobId.
     * @param numRecords number of recors for this particular jobId.
     * @param isJobDone  true if any jobType is completed.
     * @return true if any jobType is completed.
     */
    private boolean updateChildJobsForValidationFailure(String jobId, Map<String, Object> jobStatus, Integer numRecords,
            boolean isJobDone) {
        List<String> jobsToBeUpdated = new ArrayList<>();
        List<String> childJobs = (List<String>) jobStatus.get(CHILD_JOB_LIST);
        for (String childJob : childJobs) {
            if (!(boolean) jobStatus.get(childJob + "_done")) {
                if (jobStatus.containsKey(childJob + "_req_count")
                        && Objects.equals(jobStatus.get(childJob + "_req_count"), jobStatus.get(childJob + "_count"))) {
                    if ((childJob.contains("lease") || childJob.contains("segment"))
                            && !Objects.equals(numRecords, jobStatus.get(DATA_INGESTION + "_count"))) {
                        continue;
                    }
                    jobStatus.put(childJob + "_done", true);
                    jobsToBeUpdated.add(childJob);
                } else if (numRecords.equals(jobStatus.get(childJob + "_count"))) {
                    jobStatus.put(childJob + "_done", true);
                    jobsToBeUpdated.add(childJob);
                }
                if ("data_validation".equals(childJob)
                        && numRecords.equals(jobStatus.get(VALIDATION_SUCCESS + "_count"))) {
                    jobsToBeUpdated.add(childJob);
                    jobStatus.put(DATA_VALIDATION_DONE, true);
                }
            }
        }
        if (!CollectionUtils.isEmpty(jobsToBeUpdated)) {
            isJobDone = true;
            dao.updateJobStatusToDb(jobId, jobsToBeUpdated, (Long) jobStatus.get(JOB_MODIFIED_TIMESTAMP));
        }
        return isJobDone;
    }

    /**
     * Creates and entry for given job_id in Redis.
     * 
     * @param inputMsg {@link Map} to get details of the process.
     * @param jobId    id for which entry is created.
     */
    private void createaNewRedisEntry(Map<String, Object> inputMsg, String jobId) {
        log.info("*******************creating a new entry for this job_id**************\n" + jobId);
        if (StringUtils.isEmpty(inputMsg.get(NUMBER_OF_RECORDS))) {
            log.error("Number of records not specified in the first request");
        }
        Integer dup_records;
        if (StringUtils.isEmpty(inputMsg.get(DUPLICATE_RECORDS))) {
            dup_records = 0;
        } else {
            dup_records = (Integer) inputMsg.get(DUPLICATE_RECORDS);
        }

        Map<String, Object> jobStatus = new HashMap<>();
        List<Map<String, Object>> childJobs = dao.getChildJobs(jobId);
        List<String> childJobsList = new ArrayList<>();

        log.info("*********************** Child Jobs are ******************\n" + childJobs);
        if (CollectionUtils.isEmpty(childJobs)) {
            return;
        }
        for (Map<String, Object> childJob : childJobs) {
            String childJobType = (String) childJob.get(JOB_TYPE);
            if ("mapping_check".equals(childJobType) || "csv_normalization".equals(childJobType)) {
                continue;
            }
            childJobsList.add(childJobType);
            if ("data_validation".equals(childJobType)) {
                jobStatus.put(VALIDATION_SUCCESS + "_count", 0);
                jobStatus.put(VALIDATION_FAILED + "_count", 0);
            } else {
                jobStatus.put(childJobType + "_count", 0);
            }
            jobStatus.put(childJobType + "_done", false);
            if (Integer.parseInt(childJob.get("order_number").toString()) == childJobs.size()) {
                jobStatus.put(PARENT_JOB_TYPE, childJob.get(PARENT_JOB_TYPE));
            }
        }
        jobStatus.put(CHILD_JOB_LIST, childJobsList);
        jobStatus.put(NUMBER_OF_RECORDS, inputMsg.get(NUMBER_OF_RECORDS));
        jobStatus.put(JOB_START_TIMESTAMP, inputMsg.get(com.sproutloud.starter.stream.StringUtils.IN_TIME));
        jobStatus.put(EMAIL_CERTIFICATION_REQ_COUNT, 0);
        jobStatus.put(PHONE_CERTIFICATION_REQ_COUNT, 0);
        jobStatus.put(ADDRESS_CERTIFICATION_REQ_COUNT, 0);
        jobStatus.put(LEASE_DATA_INGESTION_REQ_COUNT, 0);
        jobStatus.put(SEGMENT_DATA_INGESTION_REQ_COUNT, 0);
        jobStatus.put(RECIPIENT_INSERT_COUNT, 0);
        jobStatus.put(RECIPIENT_UPDATE_COUNT, 0);
        jobStatus.put(DUPLICATE_RECORDS, dup_records);
        redisObject.save(jobId, jobStatus);
    }

}
