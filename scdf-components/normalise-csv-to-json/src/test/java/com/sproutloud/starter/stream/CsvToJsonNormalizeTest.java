package com.sproutloud.starter.stream;

import static com.sproutloud.starter.stream.constants.ApplicationConstants.DATA_TYPE;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.FIELD_FORMAT;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.FIELD_NAME;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.FIELD_TYPE;
import static com.sproutloud.starter.stream.constants.ApplicationConstants.IS_REQUIRED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import com.sproutloud.starter.stream.constants.ApplicationConstants;
import com.sproutloud.starter.stream.dao.impl.FieldsDaoImpl;
import com.sproutloud.starter.stream.gcp.FileReader;
import com.sproutloud.starter.stream.util.FileHelper;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link CsvToJsonNormalize}.
 * 
 * @author mgande
 *
 */
@ExtendWith(MockitoExtension.class)
class CsvToJsonNormalizeTest {

    @InjectMocks
    private CsvToJsonNormalize normalize;

    @Mock
    private FileHelper fielHelper;

    @Mock
    private FileReader fileReader;

    @Mock
    private FieldsDaoImpl dao;

    @Spy
    private ObjectMapper mapper;

    /**
     * Tests if field details are prepared properly.
     */
    @Test
    void testGetFieldDetails() {
        Map<String, Object> input = new HashMap<>();
        input.put(ApplicationConstants.LIST_ID, "listId");
        Map<String, Integer> fieldIndexMap = getFieldIndexMap();
        List<Map<String, String>> fieldDetailsFromDb = new ArrayList<>();
        fieldDetailsFromDb.add(prepareFieldDetailFromDb("first_name"));
        fieldDetailsFromDb.add(prepareFieldDetailFromDb("address1"));
        fieldDetailsFromDb.add(prepareFieldDetailFromDb("email"));
        Map<String, Map<String, String>> fieldDetails = new HashMap<>();
        fieldDetails.put("first_name", prepareFieldDetail(new HashMap<String, String>()));
        fieldDetails.put("address1", prepareFieldDetail(new HashMap<String, String>()));
        fieldDetails.put("email", prepareFieldDetail(new HashMap<String, String>()));
        when(dao.getListFieldsData("listId", fieldIndexMap.keySet())).thenReturn(fieldDetailsFromDb);
        assertEquals(fieldDetails, normalize.getFieldDetails(input, fieldIndexMap));
    }

    /**
     * Tests if segmentSegmentIdMap and dedupeHashFieldsDataMap are prepared properly.
     * 
     * @throws IOException
     */
    @Test
    void testNormalizeInput() throws IOException {
        Map<String, Object> input = new HashMap<>();
        input.put(ApplicationConstants.LIST_ID, "listId");
        input.put(ApplicationConstants.FILE, "fileLocation");
        input.put(ApplicationConstants.CONTAINS_HEADERS, "true");
        input.put(ApplicationConstants.USER, "user");
        input.put(ApplicationConstants.ACCOUNT_ID, "accountId");

        List<String> dedupeFields = new ArrayList<>();
        dedupeFields.add("first_name");
        dedupeFields.add("email");

        Map<String, Integer> fieldIndexMap = getFieldIndexMap();

        String[] strings = { "mani,add1,m@gmail.com,1234567,tp1|tp2,tp3,gp1,gp2", "mani,add1,m@gmail.com,1234567,tp4,tp5|tp6,gp3|gp4,",
                "mani1,add1,m1@gmail.com,1234567,tp7,,gp5|gp6,gp7" };
        InputStream inputStream = new ByteArrayInputStream(
                String.join(System.lineSeparator(), Arrays.asList(strings)).getBytes(StandardCharsets.UTF_8));
        CSVReader reader = new CSVReader(new BufferedReader(new InputStreamReader(inputStream)));

        Map<String, String> segmentSegmentIdMap = new HashMap<>();
        Map<Integer, Map<String, Object>> dedupeHashFieldsDataMap = new HashMap<>();
        when(fileReader.readCsv("fileLocation")).thenReturn(inputStream);
        when(fielHelper.getCsvReader(inputStream, 1)).thenReturn(reader);
        when(dao.getSegmentDetails("listId")).thenReturn(new HashMap<>());
        when(dao.getSequenceNumber(any(), any())).thenReturn(7);
        doNothing().when(dao).insertSegmentDetails(any());
        doNothing().when(dao).insertTpAccountDetails(any());

        normalize.normalizeInput(input, dedupeFields, fieldIndexMap, segmentSegmentIdMap, dedupeHashFieldsDataMap);

        assertTrue(segmentSegmentIdMap.keySet().containsAll(Arrays.asList(new String[] { "gp1", "gp2", "gp3", "gp4", "gp5", "gp6", "gp7" })));
        assertEquals(2, dedupeHashFieldsDataMap.size());
        verify(fileReader).readCsv("fileLocation");
        verify(fielHelper).getCsvReader(inputStream, 1);
        verify(dao).getSegmentDetails("listId");
        verify(dao).getSequenceNumber(any(), any());
        verify(dao).insertSegmentDetails(any());
        verify(dao).insertTpAccountDetails(any());
    }

    /**
     * Tests if getId gives proper id.
     */
    @Test
    void testGetId() {
        ReflectionTestUtils.setField(normalize, "formatter", new SimpleDateFormat("YYMM"));
        String output = "AB" + new SimpleDateFormat("YYMM").format(new Date()) + String.format("%010d", 10);
        assertEquals(output, normalize.getId(10, "AB"));
    }

    /**
     * Prepares {@link Map} of field index map.
     * 
     * @return {@link Map} with key as filed and value as index.
     */
    private Map<String, Integer> getFieldIndexMap() {
        Map<String, Integer> fieldIndexMap = new HashMap<>();
        fieldIndexMap.put("first_name", 1);
        fieldIndexMap.put("address1", 2);
        fieldIndexMap.put("email", 3);
        fieldIndexMap.put("mobile", 4);
        fieldIndexMap.put("tp_id_2", 5);
        fieldIndexMap.put("tp_id_1", 6);
        fieldIndexMap.put("group_2", 7);
        fieldIndexMap.put("group_1", 8);
        return fieldIndexMap;
    }

    /**
     * Fills field details.
     * 
     * @param detail {@link Map} to be updated.
     * @return {@link Map} updated detail.
     */
    private Map<String, String> prepareFieldDetail(Map<String, String> detail) {
        detail.put(DATA_TYPE, "TEXT");
        detail.put(FIELD_TYPE, "DEFAULT");
        detail.put(FIELD_FORMAT, "PROPERCASE");
        detail.put(IS_REQUIRED, "true");
        return detail;
    }

    /**
     * Prepares mock database field details data.
     * 
     * @param fieldName for which details are prepared.
     * @return {@link Map} of field details.
     */
    private Map<String, String> prepareFieldDetailFromDb(String fieldName) {
        Map<String, String> detail = new HashMap<>();
        prepareFieldDetail(detail);
        detail.put(FIELD_NAME, fieldName);
        return detail;
    }

}
