package com.sproutloud.starter.stream;

import static com.sproutloud.starter.stream.constants.ApplicationConstants.COUNTRY;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.INVALID_STATUS;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.MOBILE;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.NO_VALUE;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.SMS_MESSAGE;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.SMS_STATUS;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.VALID_STATUS;

import lombok.extern.log4j.Log4j2;

import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.Map;
import java.util.regex.Pattern;

/**
 * Certifies the mobile Data.
 * 
 * @author mgande
 *
 */
@Log4j2
@Component
public class PhoneCertification {

    /**
     * {@link Pattern} for verifying if the mobile is valid.
     */
    private final Pattern mobilePattern = Pattern.compile("^\\d{7,14}$");

    /**
     * Certifies the mobile Data and updates the status accordingly
     * 
     * @param countryCode in the fieldsData
     * @param fieldsData  Map with key as field-name and value as field-value.
     * @return String to specify status of the certification.
     */
    public String certifyPhone(String countryCode, Map<String, Object> fieldsData) {
        log.debug("Certifying mobile. \n");
        if (hasMobile(fieldsData)) {
            if ((!hasCountry(fieldsData)) && StringUtils.isEmpty(countryCode)) {
                log.debug("No country and countryCode is available. Setting sms status as INVALID. \n");
                setSmsStatus(fieldsData, false);
                return INVALID_STATUS;
            } else {
                validateMobile(fieldsData);
                if (!hasCountry(fieldsData) && VALID_STATUS.equals(fieldsData.get(SMS_STATUS))) {
                    log.debug("Updating country with countryCode \n");
                    fieldsData.put("country", countryCode);
                }
                return String.valueOf(fieldsData.get(SMS_STATUS));
            }
        } else {
            log.debug("Setting sms status as NO_VALUE \n");
            setSmsStatus(fieldsData, true);
            return NO_VALUE;
        }
    }

    /**
     * Validates the mobile value with the pattern and sets status accordingly.
     * 
     * @param fieldsMap Map with key as field-name and value as field-value.
     */
    private void validateMobile(Map<String, Object> fieldsMap) {
        String mobileValue = String.valueOf(fieldsMap.get("mobile")).replaceAll("[+()-. x]", "");
        if (mobilePattern.matcher(mobileValue).find()) {
            log.debug("Setting sms status as VALID \n");
            fieldsMap.put(SMS_STATUS, VALID_STATUS);
            fieldsMap.put(SMS_MESSAGE, "Certified by Source");
        } else {
            log.debug("Setting sms status as INVALID \n");
            fieldsMap.put(SMS_STATUS, INVALID_STATUS);
            fieldsMap.put(SMS_MESSAGE, "INVALID MOBILE NUMBER SPECIFIED");
        }

    }

    /**
     * Sets the sms status based on mobile field availability.
     * 
     * @param fieldsMap   Map with key as field-name and value as field-value.
     * @param hasNoMobile boolean specifying if mobile is present or not.
     */
    private void setSmsStatus(Map<String, Object> fieldsMap, boolean hasNoMobile) {
        if (hasNoMobile) {
            log.debug("Setting sms status as NOVALUE \n");
            fieldsMap.put(SMS_STATUS, NO_VALUE);
            fieldsMap.put(SMS_MESSAGE, "No mobile number available");
        } else {
            log.debug("Setting sms status as INVALID \n");
            fieldsMap.put(SMS_STATUS, INVALID_STATUS);
            fieldsMap.put(SMS_MESSAGE, "INVALID COUNTRY CODE SPECIFIED");
        }
    }

    /**
     * Checks if mobile is available and not empty in {@link Map} fieldValueMap.
     * 
     * @param fieldValueMap Map with key as field-name and value as field-value.
     * @return boolean, true if mobile exists.
     * 
     */
    private boolean hasMobile(Map<String, Object> fieldValueMap) {
        return (fieldValueMap.containsKey(MOBILE) && !isBlank(fieldValueMap.get(MOBILE)));
    }

    /**
     * Checks if country is available and not empty in {@link Map} fieldValueMap.
     * 
     * @param fieldValueMap Map with key as field-name and value as field-value.
     * @return boolean, true if country exists.
     * 
     */
    private boolean hasCountry(Map<String, Object> fieldValueMap) {
        return (fieldValueMap.containsKey(COUNTRY) && !isBlank(fieldValueMap.get(COUNTRY)));
    }

    /**
     * Checks if incoming string is blank.
     * 
     * @return boolean, true if blank
     */
    private boolean isBlank(Object value) {
        String valueStr = String.valueOf(value);
        if (StringUtils.isEmpty(valueStr)) {
            return true;
        }
        for (int i = 0; i < valueStr.length(); i++) {
            if (!Character.isWhitespace(valueStr.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}
