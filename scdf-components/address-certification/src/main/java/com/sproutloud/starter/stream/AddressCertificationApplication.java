package com.sproutloud.starter.stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sproutloud.starter.stream.pool.SatoriConnection;
import static com.sproutloud.starter.stream.properties.ApplicationConstants.*;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.integration.annotation.Transformer;
import org.springframework.messaging.Message;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.Map;

/**
 * Application class for address certification
 *
 * @author rgupta
 */
@Log4j2
@SpringBootApplication
@EnableBinding(Processor.class)
public class AddressCertificationApplication {

    @Autowired
    private SatoriConnection satoriConnection;

    @Autowired
    private ObjectMapper mapper;

    /**
     * main method of the address certification application
     *
     * @param args
     */
    public static void main(String[] args) {

        SpringApplication.run(AddressCertificationApplication.class, args);
    }

    @Transformer(inputChannel = Processor.INPUT, outputChannel = Processor.OUTPUT)
    public Map<String, Object> verifyAddress(Message<?> message) throws JsonProcessingException {
        long startTime = System.currentTimeMillis();
        JsonNode jsonNode = mapper.readTree((String) message.getPayload());
        Map<String, Object> processInput = mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
        });
        processInput.put(com.sproutloud.starter.stream.StringUtils.IN_TIME, System.currentTimeMillis());
        log.debug("********* Address Certification Started: ******************* \n" + processInput);

        certifyAddress(processInput);
        log.debug("********* Address Certification Ended in : ******************* \n" + (System.currentTimeMillis() - startTime));
        processInput.put(com.sproutloud.starter.stream.StringUtils.JOB_TYPE, "address_certification");
        processInput.put(com.sproutloud.starter.stream.StringUtils.OUT_TIME, System.currentTimeMillis());
        return processInput;
    }

    /**
     * Method to fetch the required address fields and then all certification method for that input
     *
     * @param processInput
     * @return
     */
    private Map<String, Object> certifyAddress(Map<String, Object> processInput) {
        Map<String, Object> fieldValueMap = (Map<String, Object>) processInput.get("fields_data");
        if (!CollectionUtils.isEmpty(fieldValueMap)) {
            String address1 = (String) fieldValueMap.get(ADDRESS1);
            String address2 = (String) fieldValueMap.get(ADDRESS2);
            String city = (String) fieldValueMap.get(CITY);
            String state = (String) fieldValueMap.get(STATE);
            String zip = (String) fieldValueMap.get(ZIP);
            String data = "\t" + address1 + '\t' + address2 + '\t' + city + '\t' + state + '\t' + zip + '\n';
            String requestCode = INPUTMSGCODE + String.valueOf(data.length()) + data;
            String satoriResponse = satoriConnection.sendMessage(requestCode, false);
            if (!StringUtils.isEmpty(satoriResponse)) {
                String cassCode = "";
                String cassMessage = "";
                String[] responseComponents = satoriResponse.split("\t");
                if (responseComponents[0].contains(OUTPUTMSGCODE) && responseComponents.length >= 8) {
                    address1 = responseComponents[1];
                    address2 = responseComponents[2];
                    city = responseComponents[3];
                    state = responseComponents[4];
                    zip = responseComponents[5];
                    cassCode = responseComponents[6];
                    cassMessage = responseComponents[7];

                } else {
                    address1 = responseComponents[0];
                    address2 = responseComponents[1];
                    city = responseComponents[2];
                    state = responseComponents[3];
                    zip = responseComponents[4];
                    cassCode = responseComponents[5];
                    if (responseComponents.length >= 7) {
                        cassMessage = responseComponents[6];
                    }
                }

                // after standarization, the address2 field might contain
                // address1 and address1 might be empty. Check if this is the case
                // and populate address1 with value in address2
                if (StringUtils.isEmpty(address1) && !StringUtils.isEmpty(address2)) {
                    address1 = address2;
                    address2 = "";
                }

                Integer cassCodeInt = Integer.parseInt(cassCode);

                // Codes 0 - 99 and 500 - 599 are considered successful.
                if (((cassCodeInt >= 0 && cassCodeInt < 100) || (cassCodeInt >= 500 && cassCodeInt < 600)) && !StringUtils.isEmpty(address1)) {

                    // mark address Valid
                    fieldValueMap.put(MAIL_STATUS, VALID_MAIL_STATUS);
                    fieldValueMap.put(MAIL_MESSAGE, "Address and Zip Found");
                } else {
                    // mark address Invalid
                    fieldValueMap.put(MAIL_STATUS, INVALID_MAIL_STATUS);

                    cassMessage = cassMessage.replace("\n", "").trim();
                    cassMessage = StringUtils.isEmpty(cassMessage) ? "Invalid Address or Postal code" : cassMessage;
                    fieldValueMap.put(MAIL_MESSAGE, cassMessage);
                }

                fieldValueMap.put(ADDRESS1, address1);
                fieldValueMap.put(ADDRESS2, address2);
                fieldValueMap.put(CITY, city);
                fieldValueMap.put(STATE, state);
                fieldValueMap.put(ZIP, zip);
                processInput.put(com.sproutloud.starter.stream.StringUtils.ADDRESS_CERTIFICATION, "done");
                processInput.put("fields_data", fieldValueMap);

            } else {
                log.error("Empty Satori response received");
                processInput.put(com.sproutloud.starter.stream.StringUtils.ADDRESS_CERTIFICATION, "skipped");
            }
        } else {
            log.error("Empty Field Value Map in the input data");
            processInput.put(com.sproutloud.starter.stream.StringUtils.ADDRESS_CERTIFICATION, "skipped");

        }
        return processInput;
    }

}
