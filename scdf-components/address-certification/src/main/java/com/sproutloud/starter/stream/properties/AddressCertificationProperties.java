package com.sproutloud.starter.stream.properties;

import lombok.Data;

import org.springframework.context.annotation.Configuration;

/**
 * holds the default values for thread pool and satori connection pool size for address certification
 * 
 * @author rgupta
 *
 */
@Data
@Configuration
public class AddressCertificationProperties {

    /**
     * IP address of the satori instance used
     */
    public final String satoriIp = "10.128.0.30";

    /**
     * Port of the satori instance used
     */
    public final Integer satoriPort = 5150;

    /**
     * Licence used for accessing the satori instance used
     */
    public final String satoriLicence = "1KI69JU-DDK25W-EHA1U";

}
