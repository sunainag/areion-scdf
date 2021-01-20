package com.sproutloud.starter.stream.util;

/**
 * Constants from the input value used in the application
 *
 * @author sgoyal
 */
public class ApplicationConstants {
    /**
     * Database table in which incoming json data is saved
     */
    public static final String TABLE = "target_table";
    /**
     * Name of the database in which incoming json data is saved
     */
    public static final String DATABASE = "target_db";
    /**
     * Database column values in the input json, for which data is saved to given table
     */
    public static final String FIELDS_DATA = "fields_data";

    /**
     * Decides on the basis of the value of this flag if data needs to be inserted or updated
     */
    public static final String ROUTER_FLAG = "router_flag";
    /**
     * List id from input
     */
    public static final String LIST_ID = "list_id";
    /**
     * Recipient id from input
     */
    public static final String RECIPIENT_ID = "recipient_id";
    /**
     * Locality code from input
     */
    public static final String LOCALITY_CODE = "locality_code";
    /**
     * User who created the input data
     */
    public static final String CREATED_BY = "created_by";
    /**
     * User who modified the input data
     */
    public static final String MODIFIED_BY = "modified_by";
    /**
     * Modified op from input
     */
    public static final String MODIFIED_OP = "modified_op";

    /**
     * Job id
     */
    public static final String JOB_ID = "job_id";
}
