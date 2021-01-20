package com.sproutloud.starter.stream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.log4j.Log4j2;

import org.apache.commons.text.WordUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Transforms the values of the contact details Map to respective formats.
 * 
 * @author mgande
 *
 */
@Log4j2
@Component
public class DataTransformer {

    /**
     * {@link Pattern} for numeric verification.
     */
    private final Pattern numericPattern = Pattern.compile("-?\\d+(\\.\\d+)?");
    /**
     * {@link SimpleDateFormat} for verifying if the given value is date.
     */
    private final SimpleDateFormat sdf = new SimpleDateFormat();

    /**
     * {@link ObjectMapper} for converting Object to required type.
     */
    @Autowired
    private ObjectMapper mapper;

    /**
     * Transforms the values of each entry of fields data with respect to field details.
     * 
     * @param input map of the incoming request.
     */
    public void transformData(Map<String, Object> input) {
        log.debug("Formatting the details to given data type");
        Map<String, Object> fieldsData = mapper.convertValue(input.get("fields_data"), new TypeReference<Map<String, Object>>() {
        });
        if (Objects.nonNull(input.get("field_details"))) {
            Map<String, Map<String, String>> fieldsDetails = mapper.convertValue(input.get("field_details"),
                    new TypeReference<Map<String, Map<String, String>>>() {
                    });
            if (!CollectionUtils.isEmpty(fieldsDetails)) {
                // Looping through each entry of fieldsData map and updating the value according to the fieldDetails
                // available for that particular field.
                fieldsData.putAll(fieldsData.entrySet().stream().collect(Collectors.toMap(Entry::getKey, map -> {
                    Map<String, String> fieldDetails = fieldsDetails.get(map.getKey());
                    if (!CollectionUtils.isEmpty(fieldDetails)) {
                        if (Objects.nonNull(fieldDetails.get("data_type")) && "BOOLEAN".equals(fieldDetails.get("data_type"))) {
                            return formatBooleanField(String.valueOf(map.getValue()), fieldDetails);
                        } else {
                            return formatField(String.valueOf(map.getValue()), fieldDetails);
                        }
                    }
                    return map.getValue();
                })));
                String dedupeHash = getDedupeHash(input.get("dedupe_fields"), fieldsData);
                if (Objects.nonNull(dedupeHash)) {
                    fieldsData.put("dedupe_hash", dedupeHash);
                }
                input.put("fields_data", fieldsData);
            }
        }

    }

    private String getDedupeHash(Object dedupeFields, Map<String, Object> fieldsData) {
        List<Object> dedupeFieldsList = mapper.convertValue(dedupeFields, new TypeReference<List<Object>>() {
        });
        String dedupeString = dedupeFieldsList.stream().map(field -> String.valueOf(fieldsData.get(field))).collect(Collectors.joining());
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        byte[] bytesOfDigest = md.digest(dedupeString.toLowerCase().getBytes());
        return DatatypeConverter.printHexBinary(bytesOfDigest);
    }

    /**
     * Formats the incoming boolean value based on field details provided and returns formatted value.
     * 
     * @param fieldDetails details of the boolean field to be formatted.
     * @param value        to be formatted.
     * @return String, formatted value.
     * 
     */
    private String formatBooleanField(String value, Map<String, String> fieldDetails) {
        if (StringUtils.isEmpty(value) || Objects.equals(value, "NULL")) {
            if (Objects.equals(fieldDetails.get("field_type"),"SYSTEM") || Objects.equals(fieldDetails.get("field_type"),"DEFAULT")) {
                value = "false";
            } else {
                value = "";
            }
        }
        return value;
    }

    /**
     * Formats the incoming value based on field details provided and returns the formatted value.
     * 
     * @param fieldValue   to be formatted.
     * @param fieldDetails details of the field to be formatted.
     * @return String, formatted value.
     */
    private String formatField(String fieldValue, Map<String, String> fieldDetails) {
        if (StringUtils.isEmpty(fieldValue) || Objects.isNull(fieldDetails)) {
            return fieldValue;
        }
        fieldValue = fieldValue.replace(System.lineSeparator(), "").replaceAll("\t", "").trim();
        String fieldDataType = fieldDetails.get("data_type");
        if (!StringUtils.isEmpty(fieldDataType)) {
            switch (fieldDataType) {
            case "TEXT":
                fieldValue = formatToGivenCase(fieldDetails.get("field_format"), fieldValue);
                break;
            case "NUMERIC":
                if (!isNumeric(fieldValue))
                    fieldValue = "";
                break;
            case "DATE":
                String validDate = convertValidDate(fieldValue, "MM/dd/yyyy");
                fieldValue = (validDate == null ? "" : validDate);
                break;
            default:
                break;
            }
        }
        return fieldValue;
    }

    /**
     * Formats the string date to the given format and returns formatted start date.
     * 
     * @param strDate    start date to be validated.
     * @param dateFormat format to which the start date has to be converted.
     * @return String, date formatted to given format.
     */
    private String convertValidDate(String strDate, String dateFormat) {
        String formattedDate = null;
        if (!StringUtils.isEmpty(strDate) && !StringUtils.isEmpty(dateFormat)) {
            sdf.applyPattern(dateFormat);
            sdf.setLenient(false);
            try {
                formattedDate = sdf.format(sdf.parse(strDate));
            } catch (ParseException e) {
                log.debug("Unparseable date: " + strDate);
            }
        }
        return formattedDate;
    }

    /**
     * Checks if the given number is valid.
     * 
     * @param fieldValue number to be validated.
     * @return boolean returns true if incoming strNum is number.
     */
    private boolean isNumeric(String fieldValue) {
        return !StringUtils.isEmpty(fieldValue) && numericPattern.matcher(fieldValue).matches();
    }

    /**
     * Formats the text value based on the format provided.
     * 
     * @param fieldFormat format of the field.
     * @param text        value of the text field to be formatted to given format.
     * @return String, formatted text value.
     */
    private String formatToGivenCase(String fieldFormat, String text) {
        if (StringUtils.isEmpty(fieldFormat)) {
            return text;
        }
        switch (fieldFormat) {
        case "PROPERCASE":
            text = WordUtils.capitalizeFully(text);
            break;
        case "UPPERCASE":
            text = text.toUpperCase();
            break;
        case "LOWERCASE":
            text = text.toLowerCase();
            break;
        default:
            log.debug("No formatting of given data type is not handled by the current system");
            break;
        }
        return text;
    }

}
