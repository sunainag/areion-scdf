package com.sproutloud.starter.stream.gcp;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import java.io.IOException;

/**
 * Google cloud storage connection client
 *
 */
@Configuration
public class StorageConfig {

    @Value("${spring.cloud.gcp.project-id:dev-slapp}")
    private String projectId;

    @Value("${spring.cloud.gcp.credentials.location}")
    private String credentialsLocation;

    @Autowired
    private ResourceLoader resourceLoader;

    /**
     * Builds a storage client instance using provided credential file and project-id
     * 
     * @return storage instance; registers it as bean
     * @throws IOException if credential file cannot be found
     */
    @Bean
    public Storage storage() throws IOException {
        return StorageOptions.newBuilder().setCredentials(loadCredentials()).setProjectId(projectId).build().getService();
    }

    private Credentials loadCredentials() throws IOException {
        Resource creadentialsFile = resourceLoader.getResource(credentialsLocation);
        return GoogleCredentials.fromStream(creadentialsFile.getInputStream());
    }
}
