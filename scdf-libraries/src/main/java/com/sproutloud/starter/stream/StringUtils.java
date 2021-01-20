package com.sproutloud.starter.stream;

/**
 * Util containing the constants used by the processor
 *
 * @author sgoyal
 */
public class StringUtils {
    /**
     * If certification is required
     */
    public static final String IS_CERTIFICATION_REQUIRED = "required";
    /**
     * If certification is not required
     */
    public static final String IS_CERTIFICATION_NOT_REQUIRED = "not_required";
    /**
     * Email certification route
     */
    public static final String EMAIL_CERTIFICATION = "email_certification";
    /**
     * Phone certification route
     */
    public static final String PHONE_CERTIFICATION = "phone_certification";
    /**
     * Address certification route
     */
    public static final String ADDRESS_CERTIFICATION = "address_certification";

    /**
     * If cass is required
     */
    public static final String IS_CASS_REQUIRED = "is_cass_required";

    /**
     * In time
     */
    public static final String IN_TIME = "in_time";

    /**
     * Out time
     */
    public static final String OUT_TIME = "out_time";

    /**
     * Job Type
     */
    public static final String JOB_TYPE = "job_type";

    /**
     * Create private constructor to avoid instantiation outside the class and ensure that the class is used as util.
     */
    private StringUtils() {
    }
}
