package com.sproutloud.starter.stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sproutloud.starter.stream.constants.ApplicationConstants;
import com.sproutloud.starter.stream.dao.impl.UpdateDaoImpl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.util.Map;

@ExtendWith(MockitoExtension.class)
public class ListUpdateServiceTests {

    @InjectMocks
    private ListUpdateService service;

    @Mock
    private UpdateDaoImpl dao;

    @Spy
    private ObjectMapper mapper;

    private Map<String, Object> inputMap;

    /**
     * Prepars input for each test.
     * 
     * @throws JsonProcessingException if unable to parse response.
     * @throws SQLException            when create array fails.
     */
    @BeforeEach
    void init() throws JsonProcessingException, SQLException {
        String input = "{\n" + 
                "    \"target_table\": \"list_data_ac20060000000012\",\n" + 
                "    \"target_db\": \"lm2_dev\",\n" + 
                "    \"account_id\": \"AC20060000000012\",\n" + 
                "    \"fields_data\": {\n" + 
                "        \"first_name\": \"Naveen\",\n" + 
                "        \"email\": \"nave@tes.cd\",\n" + 
                "        \"address1\": \"AFF\",\n" + 
                "        \"address2\": \"AEE\",\n" + 
                "        \"city\": \"HYY\",\n" + 
                "        \"state\": \"JJU\",\n" + 
                "        \"zip\": \"KKOI\",\n" + 
                "        \"mobile\": \"9899\",\n" + 
                "        \"recipient_id\": \"RC20070000000998\",\n" + 
                "        \"list_id\": \"LI20080000000401\",\n" + 
                "        \"locality_code\": \"SL_US\",\n" + 
                "        \"job_id\": \"RC20070000000998\",\n" + 
                "        \"recipient_source\": \"USER\",\n" + 
                "        \"created_by\": \"\",\n" + 
                "        \"modified_by\": \"sluser\",\n" + 
                "        \"modified_op\": \"I\",\n" + 
                "        \"email_status\": \"VALID\",\n" + 
                "        \"email_message\": \"Valid Email address\",\n" + 
                "        \"sms_status\": \"VALID\",\n" + 
                "        \"sms_message\": \"Valid mobile number\",\n" + 
                "        \"mail_status\": \"VALID\",\n" + 
                "        \"mail_message\": \"Valid address\",\n" + 
                "        \"dedupe_hash\": \"we32dsf3\"\n" + 
                "    },\n" + 
                "    \"lease_data\": {\n" + 
                "        \"target_table\": \"lease_data_ac20060000000002\",\n" + 
                "        \"tp_accounts\": [\n" + 
                "            \"tp1\",\n" + 
                "            \"tp2\"\n" + 
                "        ]\n" + 
                "    },\n" + 
                "    \"segment_data\": {\n" + 
                "        \"target_table\": \"list_set_data_ac20060000000002\",\n" + 
                "        \"segments\": [\n" + 
                "            \"LI20070000000012\",\n" + 
                "            \"LI20070000000013\"\n" + 
                "        ]\n" + 
                "    },\n" + 
                "    \"router_flag\": \"update\",\n" + 
                "    \"db_fields\": {\n" + 
                "        \"first_name\": \"Naveen1\",\n" + 
                "        \"email\": \"nave@tes.cd1\",\n" + 
                "        \"address1\": \"AFF1\",\n" + 
                "        \"address2\": \"\",\n" + 
                "        \"city\": \"HYY1\",\n" + 
                "        \"state\": \"JJU1\",\n" + 
                "        \"zip\": \"KKOI1\",\n" + 
                "        \"mobile\": \"88\",\n" + 
                "        \"recipient_id\": \"RC20070000000998\",\n" + 
                "        \"list_id\": \"LI20080000000401\",\n" + 
                "        \"locality_code\": \"SL_US\",\n" + 
                "        \"job_id\": \"{SJ20080000001217,SJ20080000001218}\",\n" + 
                "        \"recipient_source\": \"{USER, USER1}\",\n" + 
                "        \"created_by\": \"sluser\",\n" + 
                "        \"modified_by\": \"sluser\",\n" + 
                "        \"modified_op\": \"I\",\n" + 
                "        \"email_status\": \"VALID\",\n" + 
                "        \"email_message\": \"Valid Email address\",\n" + 
                "        \"sms_status\": \"VALID\",\n" + 
                "        \"sms_message\": \"Valid mobile number\",\n" + 
                "        \"mail_status\": \"VALID\",\n" + 
                "        \"mail_message\": \"Valid address\",\n" + 
                "        \"dedupe_hash\": \"we32dsf3\"   \n" + 
                "    }\n" + 
                "}";
        JsonNode inputNode = mapper.readTree(input);
        inputMap = mapper.convertValue(inputNode, new TypeReference<Map<String, Object>>() {
        });
        
    }

    /**
     * Integration tests for the list data update success case.
     * 
     * @throws SQLException
     */
    @Test
    void testUpdateListData() throws SQLException {
        doNothing().when(dao).updateListData(any(), any(), any(), any(), any());
        when(dao.createArray(any())).thenReturn(null);
        service.updateList(inputMap);
        verify(dao, times(2)).createArray(any());
        verify(dao).updateListData(any(), any(), any(), any(), any());
    }

    /**
     * Integration tests for the list data update when no table is given.
     * 
     * @throws SQLException
     */
    @Test
    void testUpdateListDataNoTable() throws SQLException {
        inputMap.remove(ApplicationConstants.TABLE);
        service.updateList(inputMap);
        verify(dao, never()).createArray(any());
        verify(dao, never()).updateListData(any(), any(), any(), any(), any());
    }
}
