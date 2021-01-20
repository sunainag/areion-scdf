package com.sproutloud.starter.stream;

import static com.sproutloud.starter.stream.constants.ApplicationConstants.EMAIL;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.EMAIL_MESSAGE;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.EMAIL_STATUS;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.INVALID_STATUS;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.NO_VALUE;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.VALID_STATUS;

import lombok.extern.log4j.Log4j2;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;

/**
 * Certifies the email Data.
 * 
 * @author mgande
 *
 */
@Log4j2
@Component
public class EmailCertification {

    /**
     * regex string for email size verification.
     */
    @Value("${regex.email.size}")
    private String emailSizeRegex;

    /**
     * regex string for verifying if mailbox is valid.
     */
    @Value("${regex.email.mailbox}")
    private String mailboxRegex;

    /**
     * regex string for verifying if domain is valid.
     */
    @Value("${regex.email.domain1}")
    private String domainPattern1Regex;

    /**
     * alternative regex string for verifying if domain is valid.
     */
    @Value("${regex.email.domain2}")
    private String domainPattern2Regex;

    /**
     * {@link Pattern} for email size verification.
     */
    private Pattern emailSizePattern;

    /**
     * {@link Pattern} for verifying if mailbox is valid.
     */
    private Pattern mailboxPattern;

    /**
     * {@link Pattern} for verifying if domain is valid.
     */
    private Pattern domainPattern1;

    /**
     * alternate {@link Pattern} for verifying if domain is valid.
     */
    private Pattern domainPattern2;

    /**
     * {@link List} of tlds (domains) accepted.
     */
    @Value("${tlds.list}")
    private List<String> tlds;

    @PostConstruct
    private void init() {
        emailSizePattern = Pattern.compile(emailSizeRegex);
        mailboxPattern = Pattern.compile(mailboxRegex);
        domainPattern1 = Pattern.compile(domainPattern1Regex);
        domainPattern2 = Pattern.compile(domainPattern2Regex);
    }

    /**
     * Certifies the email Data and updates the status accordingly
     * 
     * @param fieldsMap Map with key as field-name and value as field-value.
     * @return String to specify status of the certification.
     */
    public String certifyEmail(Map<String, Object> fieldsMap) {
        log.debug("Checking email. \n");
        if (hasEmail(fieldsMap)) {
            boolean isValid = formatEmail(String.valueOf(fieldsMap.get(EMAIL)));
            if (isValid) {
                log.debug("Setting email status as VALID \n");
                setEmailStatus(fieldsMap, VALID_STATUS);
            } else {
                log.debug("Setting email status as INVALID \n");
                setEmailStatus(fieldsMap, INVALID_STATUS);
            }
        } else {
            log.debug("Setting email status as NO_VALUE \n");
            setEmailStatus(fieldsMap, NO_VALUE);
        }
        return String.valueOf(fieldsMap.get(EMAIL_STATUS));
    }

    /**
     * Validates the email value with the required patterns and returns the response.
     * 
     * @param email to be validated.
     * @return true, if email is valid.
     */
    private boolean formatEmail(String email) {
        log.debug("Checking if the size of the email is valid and if @ is present in email \n");
        if ((!emailSizePattern.matcher(email).matches()) || (email.indexOf("@") == -1)) {
            return false;
        }
        Integer domainPos = email.indexOf("@");
        String mailBox = email.substring(0, domainPos);
        String domain = email.substring(domainPos + 1);
        log.debug("Validating the mailbox with regex \n");
        if (!mailboxPattern.matcher(mailBox).matches()) {
            return false;
        }
        log.debug("Validating the domain with regex \n");
        if (!domainPattern1.matcher(domain).matches()) {
            log.debug("Validating the domain with alternative regex \\n");
            if (!domainPattern2.matcher(domain).matches()) {
                return false;
            }
        }
        log.debug("Making sure that all the domains in email are less than 63 characters \n");
        String[] domains = domain.split("[.]");
        for (String node : domains) {
            if (node.length() > 63) {
                return false;
            }
        }
        String tld = domains[domains.length - 1];
        log.debug("Checking if email domain is present in valid tlds(domains) \n");
        return tlds.contains(tld.toUpperCase());
    }

    /**
     * Sets email status and email message details.
     * 
     * @param fieldValueMap Map with key as field-name and value as field-value.
     * @param status        to be set for email.
     */
    private void setEmailStatus(Map<String, Object> fieldValueMap, String status) {
        fieldValueMap.put(EMAIL_STATUS, status);
        if (NO_VALUE.equals(status)) {
            fieldValueMap.put(EMAIL_MESSAGE, "No email address available.");
        } else {
            fieldValueMap.put(EMAIL_MESSAGE, status + " email address.");
        }
    }

    /**
     * Checks if email is available and not empty in {@link Map} fieldValueMap.
     * 
     * @param fieldValueMap Map with key as field-name and value as field-value.
     * @return boolean, true if email exists.
     * 
     */
    private boolean hasEmail(Map<String, Object> fieldValueMap) {
        return (fieldValueMap.containsKey(EMAIL) && !isBlank(String.valueOf(fieldValueMap.get(EMAIL))));
    }

    /**
     * Checks if incoming string is blank.
     * 
     * @return boolean, true if blank
     */
    private boolean isBlank(String emailStr) {
        if (StringUtils.isEmpty(emailStr)) {
            return true;
        }
        for (int i = 0; i < emailStr.length(); i++) {
            if (!Character.isWhitespace(emailStr.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}
