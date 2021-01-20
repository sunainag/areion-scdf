package com.sproutloud.starter.stream;

import static com.sproutloud.starter.stream.properties.ApplicationConstants.ADDRESS1;
import static com.sproutloud.starter.stream.properties.ApplicationConstants.ADDRESS2;
import static com.sproutloud.starter.stream.properties.ApplicationConstants.CITY;
import static com.sproutloud.starter.stream.properties.ApplicationConstants.MAIL_MESSAGE;
import static com.sproutloud.starter.stream.properties.ApplicationConstants.MAIL_STATUS;
import static com.sproutloud.starter.stream.properties.ApplicationConstants.STATE;
import static com.sproutloud.starter.stream.properties.ApplicationConstants.ZIP;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sproutloud.starter.stream.pool.SatoriConnection;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.integration.support.MessageBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link AddressCertificationApplication} class.
 * 
 * @author mgande
 *
 */
@ExtendWith(MockitoExtension.class)
class AddressCertificationApplicationTests {

    @InjectMocks
    private AddressCertificationApplication app;

    @Mock
    private SatoriConnection satoriConnection;

    @Spy
    private ObjectMapper mapper;

    /**
     * Tests valid input case.
     * 
     * @throws JsonProcessingException when unable to convert input to {@link Map}
     * 
     */
    @Test
    void testVerifyAddress() throws JsonProcessingException {
        // Valid scenario with BTO in output.
        Map<String, Object> fieldsData = new HashMap<>();
        fieldsData.put(ADDRESS1, "address1");
        fieldsData.put(ADDRESS2, "address2");
        fieldsData.put(CITY, "city");
        fieldsData.put(STATE, "state");
        fieldsData.put(ZIP, "zip");
        Map<String, Object> input = new HashMap<>();
        input.put("fields_data", fieldsData);
        String satoriOutput = "BTO\taddressUpdated1\taddressUpdated2\tcity\tstate\tzip\t10\tcassMessage";
        when(satoriConnection.sendMessage(any(), eq(false))).thenReturn(satoriOutput);
        Map<String, Object> res = app.verifyAddress(MessageBuilder.withPayload(mapper.writeValueAsString(input)).build());
        assertEquals("done", res.get("address_certification"));

        // Valid scenario without BTO in output.
        satoriOutput = "addressUpdated1\taddressUpdated2\tcity\tstate\tzip\t10\tcassMessage";
        when(satoriConnection.sendMessage(any(), eq(false))).thenReturn(satoriOutput);
        res = app.verifyAddress(MessageBuilder.withPayload(mapper.writeValueAsString(input)).build());
        assertEquals("done", res.get("address_certification"));

        // Valid scenario without cass message in output.
        satoriOutput = "BTO\taddressUpdated1\taddressUpdated2\tcity\tstate\tzip\t10\t";
        when(satoriConnection.sendMessage(any(), eq(false))).thenReturn(satoriOutput);
        res = app.verifyAddress(MessageBuilder.withPayload(mapper.writeValueAsString(input)).build());
        assertEquals("done", res.get("address_certification"));

        // Valid scenario without address1 in output.
        satoriOutput = "\taddressUpdated2\tcity\tstate\tzip\t10\tcassMessage";
        when(satoriConnection.sendMessage(any(), eq(false))).thenReturn(satoriOutput);
        res = app.verifyAddress(MessageBuilder.withPayload(mapper.writeValueAsString(input)).build());
        assertEquals("done", res.get("address_certification"));

        // InValid scenario without BTO in output.
        satoriOutput = "addressUpdated1\taddressUpdated2\tcity\tstate\tzip\t104\tcassMessage";
        when(satoriConnection.sendMessage(any(), eq(false))).thenReturn(satoriOutput);
        res = app.verifyAddress(MessageBuilder.withPayload(mapper.writeValueAsString(input)).build());
        assertEquals("done", res.get("address_certification"));

        // InValid scenario without BTO, address1, address2 and cass message in output.
        satoriOutput = "\t\tcity\tstate\tzip\t501\t";
        when(satoriConnection.sendMessage(any(), eq(false))).thenReturn(satoriOutput);
        res = app.verifyAddress(MessageBuilder.withPayload(mapper.writeValueAsString(input)).build());
        assertEquals("done", res.get("address_certification"));
    }

    /**
     * Tests invalid input case.
     * 
     * @throws JsonProcessingException when unable to convert input to {@link Map}
     * 
     */
    @Test
    void testVerifyAddressInvalidInput() throws JsonProcessingException {
        Map<String, Object> input = new HashMap<>();
        input.put("key", "value");

        assertEquals("skipped", 
                app.verifyAddress(MessageBuilder.withPayload(mapper.writeValueAsString(input)).build()).get("address_certification"));
        Map<String, Object> fieldsData = new HashMap<>();
        fieldsData.put(ADDRESS1, "address1");
        input.put("fields_data", fieldsData);
        when(satoriConnection.sendMessage(any(), eq(false))).thenReturn(null);
        assertEquals("skipped", 
                app.verifyAddress(MessageBuilder.withPayload(mapper.writeValueAsString(input)).build()).get("address_certification"));
    }

}
