package com.sproutloud.starter.stream;

import static com.sproutloud.starter.stream.constants.ApplicationConstants.*;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.Storage;
import com.opencsv.CSVReader;
import com.sproutloud.starter.stream.dao.impl.FieldsDaoImpl;
import com.sproutloud.starter.stream.gcp.FileReader;
import com.sproutloud.starter.stream.util.FileHelper;

import lombok.extern.log4j.Log4j2;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Reads CSV file from Google storage. Normalizes the CSV data to required json.
 * 
 * @author mgande
 *
 */
@Log4j2
@Component
public class CsvToJsonNormalize {

    /**
     * {@link FileHelper} helper class for reading CSV.
     */
    @Autowired
    private FileHelper fielHelper;

    /**
     * {@link Storage} bean to read CSV from google storage.
     */
    @Autowired
    private FileReader fileReader;

    /**
     * {@link FieldsDaoImpl} bean to make db calls.
     */
    @Autowired
    private FieldsDaoImpl dao;

    /**
     * {@link ObjectMapper} to convert Object to required type.
     */
    @Autowired
    private ObjectMapper mapper;

    private final SimpleDateFormat formatter = new SimpleDateFormat("YYMM");

    /**
     * Reads CSV file from Google storage. Normalizes the CSV data to required json. Updates the segment and lease data in db.
     * 
     * @param input                   json format of incoming message.
     * @param dedupeFields            {@link List} of dedupe fields.
     * @param fieldIndexMap           {@link Map} of key a field name and value as index in csv.
     * @param segmentSegmentIdMap     {@link Map} with key as segmentValue and value as segmentId.
     * @param dedupeHashFieldsDataMap {@link Map} with key as dedupe hash and value as fields data.
     * @return total number of rows in csv
     * @throws IOException when unable to read CSV file.
     */
    public Integer normalizeInput(Map<String, Object> input, List<String> dedupeFields, Map<String, Integer> fieldIndexMap,
            Map<String, String> segmentSegmentIdMap, Map<Integer, Map<String, Object>> dedupeHashFieldsDataMap) throws IOException {
        String file = (String) input.get("file");
        log.debug("Fetching file from Google cloud storage bucket: " + file);
        InputStream in = fileReader.readCsv(file);
        int skipHeader = Objects.equals(input.get(CONTAINS_HEADERS), "true") ? 1 : 0;

        log.debug("Reading the CSV file. \n");
        CSVReader csvReader = fielHelper.getCsvReader(in, skipHeader);
        Set<String> uniqueTpAccounts = new HashSet<>();
        Set<String> uniqueSegments = new HashSet<>();
        log.debug("Iterating through each entry of CSV file \n ");
        Integer totalNumberOfRecords = 0;
        String[] rowValues;
        while ((rowValues = csvReader.readNext()) != null) {
            log.debug("Preparing edupe hash \n");
            totalNumberOfRecords++;
            List<String> dedupeFieldValues = new ArrayList<>();
            for (String field : dedupeFields) {
                dedupeFieldValues.add(rowValues[fieldIndexMap.get(field) - 1]);
            }
            Integer dedupeHash = Arrays.hashCode(dedupeFieldValues.toArray());
            // if the dedupeHashFieldsDataMap map already has entry with the dedupeHash then
            // value of the entry in dedupeHashFieldsDataMap will be updated
            if (dedupeHashFieldsDataMap.containsKey(dedupeHash)) {
                dedupeHashFieldsDataMap.put(dedupeHash, updateDedupeHashFieldsDataMap(fieldIndexMap, rowValues,
                        dedupeHashFieldsDataMap.get(dedupeHash), uniqueTpAccounts, uniqueSegments));
            } else {
                dedupeHashFieldsDataMap.put(dedupeHash, insertDedupeHashFieldsDataMap(fieldIndexMap, rowValues, uniqueTpAccounts, uniqueSegments));
            }
        }
        csvReader.close();

        log.debug("Getting segment details by list_id from DB \n");
        segmentSegmentIdMap.putAll(dao.getSegmentDetails((String) input.get(LIST_ID)));
        log.debug("Removing segments from uniqueSegments which are in DB \n");
        uniqueSegments.removeAll(segmentSegmentIdMap.keySet());
        Timestamp currentTs = new Timestamp(System.currentTimeMillis());
        insertLeaseSegmentDetailsToDB(input, segmentSegmentIdMap, uniqueTpAccounts, uniqueSegments, currentTs);
        return totalNumberOfRecords;
    }

    /**
     * Inserts the data to list_meta and acct_members table.
     * 
     * @param input               json format of incoming message.
     * @param segmentSegmentIdMap {@link Map} with key as segmentValue and value as segmentId.
     * @param uniqueTpAccounts    {@link Set} of unique tp accounts.
     * @param uniqueSegments      {@link Set} of unique segments.
     * @param currentTs           {@link Timestamp} of current time.
     */
    private void insertLeaseSegmentDetailsToDB(Map<String, Object> input, Map<String, String> segmentSegmentIdMap, Set<String> uniqueTpAccounts,
            Set<String> uniqueSegments, Timestamp currentTs) {
        if (!CollectionUtils.isEmpty(uniqueSegments)) {
            log.debug("Getting sequence number for list_meta \n");
            Integer listMetaSequence = dao.getSequenceNumber(uniqueSegments.size(), "list_meta_seq");
            List<String> SegmentInsertValues = new ArrayList<>();
            for (String segment : uniqueSegments) {
                SegmentInsertValues.add(getSegmentInsertString(listMetaSequence++, segment, currentTs, input, segmentSegmentIdMap));
            }
            log.debug("Inserting segment details to DB \n");
            dao.insertSegmentDetails(SegmentInsertValues);
        }

        if (!CollectionUtils.isEmpty(uniqueTpAccounts)) {
            List<String> tpAccountsInsertValues = new ArrayList<>();
            for (String account : uniqueTpAccounts) {
                tpAccountsInsertValues.add(getTpAccountInsertString(input, account, currentTs));
            }
            log.debug("Inserting tp account details to DB \n");
            dao.insertTpAccountDetails(tpAccountsInsertValues);
        }
    }

    /**
     * Fetches all the field details mapped to given list_id from db and transforms it into required {@link Map} format. Validates if all the fields
     * in mappings of input are having field details.
     * 
     * @param input         json format of incoming message.
     * @param fieldIndexMap {@link Map} of key a field name and value as index in csv.
     * @return {@link Map} with key as field name and value as field details.
     */
    public Map<String, Map<String, String>> getFieldDetails(Map<String, Object> input, Map<String, Integer> fieldIndexMap) {
        List<Map<String, String>> fieldDetailsFromDb = dao.getListFieldsData((String) input.get(LIST_ID), fieldIndexMap.keySet());
        Map<String, Map<String, String>> fieldDetails = prepareFieldDetailsMap(fieldDetailsFromDb);
        log.debug("Checking if all the fields in mappings are having field details. \n");
        for (Entry<String, Integer> entry : fieldIndexMap.entrySet()) {
            if (StringUtils.startsWithIgnoreCase(entry.getKey(), TP_ID) || StringUtils.startsWithIgnoreCase(entry.getKey(), GROUP)) {
                continue;
            }
            if (!fieldDetails.containsKey(entry.getKey())) {
                log.error("Mapping field is not present in fieldDetails. "+ entry.getKey()+" , "+fieldDetails.entrySet().stream().map(e->e.getKey()+"="+e.getValue()).collect(Collectors.joining(",","{","}")));
            }
        }
        return fieldDetails;
    }

    /**
     * Converts the field details obtained from db to required format.
     * 
     * @param fieldDetailsFromDb {@link Map} with key as field detail name and value as field detail value.
     * @return {@link Map} with key as field name and value as field details.
     */
    private Map<String, Map<String, String>> prepareFieldDetailsMap(List<Map<String, String>> fieldDetailsFromDb) {
        Map<String, Map<String, String>> fieldDetails = new HashMap<>();
        for (Map<String, String> detail : fieldDetailsFromDb) {
            Map<String, String> details = new HashMap<>();
            details.put(DATA_TYPE, detail.get(DATA_TYPE));
            details.put(FIELD_TYPE, detail.get(FIELD_TYPE));
            details.put(FIELD_FORMAT, detail.get(FIELD_FORMAT));
            details.put(IS_REQUIRED, detail.get(IS_REQUIRED));
            fieldDetails.put(detail.get(FIELD_NAME), details);
        }
        log.debug("FieldDetails prepared :"+fieldDetails);
        return fieldDetails;
    }

    /**
     * 
     * Prepares {@link Map} of field-name and field-value using {@link Map} fieldIndexMap and String array rowValues.
     * 
     * @param fieldIndexMap    {@link Map} of key a field name and value as index in csv.
     * @param rowValues        string array of field values.
     * @param uniqueTpAccounts {@link Set} of unique tp accounts.
     * @param uniqueSegments   {@link Set} of unique segments.
     * @return {@link Map} with key as field name and value as field value
     */
    private Map<String, Object> insertDedupeHashFieldsDataMap(Map<String, Integer> fieldIndexMap, String[] rowValues, Set<String> uniqueTpAccounts,
            Set<String> uniqueSegments) {
        Map<String, Object> fieldsData = new HashMap<>();
        for (Entry<String, Integer> field : fieldIndexMap.entrySet()) {
            String fieldName = field.getKey();
            Integer fieldIndex = field.getValue() - 1;
            if (StringUtils.startsWithIgnoreCase(fieldName, TP_ID)) {
                Set<String> tpAccounts = new HashSet<>();
                if (fieldsData.containsKey(TP_IDS)) {
                    tpAccounts.addAll(mapper.convertValue(fieldsData.get(TP_IDS), new TypeReference<Set<String>>() {
                    }));
                }
                getUpdatedList(rowValues[fieldIndex], tpAccounts);
                fieldsData.put(TP_IDS, tpAccounts);
                uniqueTpAccounts.addAll(tpAccounts);
            } else if (StringUtils.startsWithIgnoreCase(fieldName, GROUP)) {
                Set<String> segments = new HashSet<>();
                if (fieldsData.containsKey(SEGMENTS)) {
                    segments.addAll(mapper.convertValue(fieldsData.get(SEGMENTS), new TypeReference<Set<String>>() {
                    }));
                }
                getUpdatedList(rowValues[fieldIndex], segments);
                fieldsData.put(SEGMENTS, segments);
                uniqueSegments.addAll(segments);
            } else {
                fieldsData.put(fieldName, rowValues[fieldIndex]);
            }
        }
        return fieldsData;
    }

    /**
     * 
     * Updates {@link Map} of field-name and field-value using {@link Map} fieldIndexMap and String array rowValues.
     * 
     * @param fieldIndexMap  {@link Map} of key a field name and value as index in csv.
     * @param rowValues      string array of field values.
     * @param fieldsData     {@link Map} of key as field name and value as field value.
     * @param uniqueSegments {@link Set} of unique segments.
     * @return Updated {@link Map} with key as field name and value as field value
     */
    private Map<String, Object> updateDedupeHashFieldsDataMap(Map<String, Integer> fieldIndexMap, String[] rowValues, Map<String, Object> fieldsData,
            Set<String> uniqueTpAccounts, Set<String> uniqueSegments) {
        for (Entry<String, Integer> field : fieldIndexMap.entrySet()) {
            String fieldName = field.getKey();
            Integer fieldIndex = field.getValue() - 1;
            if (StringUtils.startsWithIgnoreCase(fieldName, TP_ID)) {
                Set<String> tpAccounts = mapper.convertValue(fieldsData.get(TP_IDS), new TypeReference<Set<String>>() {
                });
                getUpdatedList(rowValues[fieldIndex], tpAccounts);
                fieldsData.put(TP_IDS, tpAccounts);
                uniqueTpAccounts.addAll(tpAccounts);
            } else if (StringUtils.startsWithIgnoreCase(fieldName, GROUP)) {
                Set<String> segments = mapper.convertValue(fieldsData.get(SEGMENTS), new TypeReference<Set<String>>() {
                });
                getUpdatedList(rowValues[fieldIndex], segments);
                fieldsData.put(SEGMENTS, segments);
                uniqueSegments.addAll(segments);
            } else {
                String value = (String) fieldsData.get(fieldName);
                if (StringUtils.isEmpty(value)) {
                    fieldsData.put(fieldName, rowValues[fieldIndex]);
                }
            }
        }
        return fieldsData;
    }

    /**
     * Updates the list of tpAccounts/Segments.
     * 
     * @param string tpAccounts/Segments string with '|' separated.
     * @param list   {@link List} of existing tpAccounts/Segments.
     */
    private void getUpdatedList(String string, Set<String> list) {
        for (String account : string.split("\\|")) {
            account = account.trim();
            if (!StringUtils.isEmpty(account)) {
                list.add(account);
            }
        }
    }

    /**
     * Prepares the String with the values to be inserted into acct_members table.
     * 
     * @param input     json format of incoming message.
     * @param account   tp account string.
     * @param currentTs {@link Timestamp} of current time.
     * @return String of values to be inserted.
     */
    private String getTpAccountInsertString(Map<String, Object> input, String account, Timestamp currentTs) {
        String user = (String) input.get(USER);
        return "('" + currentTs + "', '" + user + "', 'I', '" + user + "', '" + currentTs + "', '" + (String) input.get(ACCOUNT_ID) + "', 'SL_US', '"
                + account + "')";
    }

    /**
     * Prepares the String with the values to be inserted into list_meta table.
     * 
     * @param listMetaSequence    {@link Integer} of sequence to prepare list_id
     * @param segment             value to be inserted.
     * @param currentTs           {@link Timestamp} of current time.
     * @param input               json format of incoming message.
     * @param segmentSegmentIdMap {@link Map} with key as segmentValue and value as segmentId.
     * @return String of values to be inserted.
     */
    private String getSegmentInsertString(Integer listMetaSequence, String segment, Timestamp currentTs, Map<String, Object> input,
            Map<String, String> segmentSegmentIdMap) {
        String user = (String) input.get(USER);
        String id = getId(listMetaSequence, "LI");
        segmentSegmentIdMap.put(segment, id);
        return "('" + currentTs + "', '" + user + "', 'I', '" + user + "', '" + currentTs + "', '" + id + "', 'SL_US', '"
                + (String) input.get(ACCOUNT_ID) + "', '" + (String) input.get(LIST_ID) + "', '" + segment + "', 'segment', '" + segment + "')";
    }

    /**
     * Generated the Id in required format.
     * 
     * @param sequenceNumber {@link Integer} sequence number.
     * @param tag            type of Id to be created.
     * @return {@link String} of id.
     */
    public String getId(Integer sequenceNumber, String tag) {
        return tag + formatter.format(new Date()) + String.format("%010d", sequenceNumber);
    }
}
