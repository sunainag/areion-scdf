package com.sproutloud.starter.stream.gcp;

import com.google.cloud.storage.Storage;

import lombok.extern.log4j.Log4j2;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.gcp.storage.GoogleStorageResource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;

/**
 * Reads the csv file from the google storage.
 * 
 * @author mgande
 *
 */
@Log4j2
@Component
public class FileReader {

    /**
     * {@link Storage} bean for google storage access.
     */
    @Autowired
    private Storage storage;

    /**
     * Reads the CSV file from google storage and returns as {@link InputStream}.
     * 
     * @param file of the csv file in google storage.
     * @return {@link InputStream} of the file read.
     */
    public InputStream readCsv(String file) {
        log.debug("Fetching file from Google cloud storage bucket: " + file);
        try {
            GoogleStorageResource resource = new GoogleStorageResource(storage, file, true);
            return resource.getInputStream();
        } catch (IOException e) {
            log.error("Unable to fetch CSV from Google Storage");
            throw new RuntimeException(e.getMessage());
        }
    }
}
