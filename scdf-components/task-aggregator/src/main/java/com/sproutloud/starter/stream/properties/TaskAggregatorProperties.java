package com.sproutloud.starter.stream.properties;

/**
 * Holds all the properties being used in task launcher application
 * 
 * @author rishabhg
 *
 */
public class TaskAggregatorProperties {

    /**
     * total number of records to be processed
     */
    public static final String NUMBER_OF_RECORDS = "number_of_records";

    /**
     * count of record email certified successfully
     */
    public static final String EMAIL_CERTIFICATION_COUNT = "email_certification_count";

    /**
     * count of record email certified successfully
     */
    public static final String PHONE_CERTIFICATION_COUNT = "phone_certification_count";

    /**
     * count of record email certified successfully
     */
    public static final String ADDRESS_CERTIFICATION_COUNT = "address_certification_count";

    /**
     * count of record deduped successfully
     */
    public static final String DEDUPE_COUNT = "dedupe_count";

    /**
     * count of record transformed successfully
     */
    public static final String TRANSFORMATION_COUNT = "transformation_count";

    /**
     * count of record inserted successfully into list_data table
     */
    public static final String LIST_INGESTION_COUNT = "list_ingestion_count";

    /**
     * count of record inserted successfully into lease_data table
     */
    public static final String LEASE_INGESTION_COUNT = "lease_ingestion_count";

    /**
     * count of record inserted successfully into segment_data table
     */
    public static final String SEGMENT_INGESTION_COUNT = "segment_ingestion_count";

    /**
     * count of record that need to be certified to phone
     */
    public static final String EMAIL_CERTIFICATION_REQ_COUNT = "email_certification_req_count";

    /**
     * count of record that need to be certified for phone
     */
    public static final String PHONE_CERTIFICATION_REQ_COUNT = "phone_certification_req_count";

    /**
     * count of record that need to be inserted to lease table
     */
    public static final String LEASE_DATA_INGESTION_REQ_COUNT = "lease_data_ingestion_req_count";

    /**
     * count of record that need to be inserted to segment table
     */
    public static final String SEGMENT_DATA_INGESTION_REQ_COUNT = "segment_data_ingestion_req_count";

    /**
     * count of record that need to be certified to email
     */
    public static final String ADDRESS_CERTIFICATION_REQ_COUNT = "address_certification_req_count";

    /**
     * flag to be used to mark validation phase as done
     */
    public static final String DATA_VALIDATION_DONE = "data_validation_done";

    /**
     * flag to be used to mark email-certification phase as done
     */
    public static final String EMAIL_CERTIFICATION_DONE = "email_certification_done";

    /**
     * flag to be used to mark phone-certification phase as done
     */
    public static final String PHONE_CERTIFICATION_DONE = "phone_certification_done";

    /**
     * flag to be used to mark address-certification phase as done
     */
    public static final String ADDRESS_CERTIFICATION_DONE = "address_certification_done";

    /**
     * flag to be used to mark transformation phase as done
     */
    public static final String TRANSFORMATION_DONE = "transformation_done";

    /**
     * flag to be used to mark transformation phase as done
     */
    public static final String DEDUPE_DONE = "dedupe_done";

    /**
     * flag to be used to mark list_ingestion phase as done
     */
    public static final String DATA_INGESTION_DONE = "data_ingestion_done";

    /**
     * flag to be used to mark lease_ingestion required
     */
    public static final String LEASE_DATA_INGESTION_REQ = "lease_data_ingestion_required";

    /**
     * flag to be used to mark segment_ingestion required
     */
    public static final String SEGMENT_DATA_INGESTION_REQ = "segment_data_ingestion_required";

    /**
     * flag to be used to mark lease_ingestion phase as done
     */
    public static final String LEASE_INGESTION_DONE = "lease_ingestion_done";

    /**
     * flag to be used to mark segment_ingestion phase as done
     */
    public static final String SEGMENT_INGESTION_DONE = "segment_ingestion_done";

    /**
     * flag to be used to mark segment_ingestion phase as done
     */
    public static final String RECIPIENT_INSERT_COUNT = "recipient_insert_count";

    /**
     * flag to be used to mark segment_ingestion phase as done
     */
    public static final String RECIPIENT_UPDATE_COUNT = "recipient_update_count";

    /**
     * child jobs required for this stream
     */
    public static final String CHILD_JOBS = "child_jobs";

    /**
     * last child job in this stream
     */
    public static final String LAST_CHILD_JOB = "last_child_job";

    /**
     * timestamp to be used to mark start of the process
     */
    public static final String JOB_START_TIMESTAMP = "job_start_timestamp";

    /**
     * timestamp to be used to mark last modified timestamp
     */
    public static final String JOB_MODIFIED_TIMESTAMP = "job_modified_timestamp";

    /**
     * job id of the job.
     */
    public static final String JOB_ID = "job_id";

    /**
     * parent job id of the job.
     */
    public static final String PARENT_JOB_TYPE = "parent_job_type";

    /**
     * job type.
     */
    public static final String JOB_TYPE = "job_type";

    /**
     * Failure status from validation
     */
    public static final String VALIDATION_FAILED = "validation_failed";

    /**
     * Success status from validation
     */
    public static final String VALIDATION_SUCCESS = "validation_success";

    /**
     * data ingestion job type
     */
    public static final String DATA_INGESTION = "data_ingestion";

    /**
     * lease data ingestion job type
     */
    public static final String LEASE_DATA_INGESTION = "lease_data_ingestion";

    /**
     * segment data ingestion job type
     */
    public static final String SEGMENT_DATA_INGESTION = "segment_data_ingestion";

    /**
     * email certification job type
     */
    public static final String EMAIL_CERTIFICATION = "email_certification";

    /**
     * phone certification job type
     */
    public static final String PHONE_CERTIFICATION = "phone_certification";

    /**
     * address certification job type
     */
    public static final String ADDRESS_CERTIFICATION = "address_certification";

    /**
     * child job list String
     */
    public static final String CHILD_JOB_LIST = "child_job_list";
    
    /**
     * duplicate reocrds in the csv
     */
    public static final String DUPLICATE_RECORDS = "duplicate_records";
}
