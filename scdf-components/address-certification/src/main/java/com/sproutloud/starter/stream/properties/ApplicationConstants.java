package com.sproutloud.starter.stream.properties;

public class ApplicationConstants {
    /**
     * value of mail status to be set if the address is valid
     */
    public static final String VALID_MAIL_STATUS = "VALID";

    /**
     * value of mail status to be set if the address is invalid
     */
    public static final String INVALID_MAIL_STATUS = "INVALID";

    /**
     * List of all the handshake messages that needs to be sent to satori
     */
    public static final String HANDSHAKEMSG1 = "BTI1" + "=";
    public static final String HANDSHAKEMSG2 = "BTI8=7\t202\t1\n";
    public static final String HANDSHAKEMSG3 = "BTI8=70\t200\tFLD_ADDRESSLINE1\tFLD_ADDRESSLINE2\tFLD_CITY\tFLD_STATE\tFLD_ZIPCODE\n";
    public static final String HANDSHAKEMSG4 = "BTI8=106\t201\tFLD_ADDRESSLINE1\tFLD_ADDRESSLINE2\tFLD_CITY\tFLD_STATE\tFLD_ZIPCODE\tFLD_ERRORCODE\tFLD_LONG_ERROR_STRING\n";
    public static final String HANDSHAKEMSG5 = "BTI8=7\t295\t0\n";
    public static final String HANDSHAKEMSG6 = "BTI8=7\t294\t1\n";
    public static final String HANDSHAKEMSG7 = "BTI2=2\t\n";

    /**
     * code used while sending the certification data to satori
     */
    public static final String INPUTMSGCODE = "BTIB=";

    /**
     * code checked while receiving the certified data from satori
     */
    public static final String OUTPUTMSGCODE = "BTO";

    /**
     * parameter to be used for address1
     */
    public static final String ADDRESS1 = "address1";

    /**
     * parameter to be used for address2
     */
    public static final String ADDRESS2 = "address2";

    /**
     * parameter to be used for city
     */
    public static final String CITY = "city";

    /**
     * parameter to be used for state
     */
    public static final String STATE = "state";

    /**
     * parameter to be used for zip
     */
    public static final String ZIP = "zip";

    /**
     * parameter to be used for mail_status
     */
    public static final String MAIL_STATUS = "mail_status";

    /**
     * parameter to be used for mail_message
     */
    public static final String MAIL_MESSAGE = "mail_message";
}
