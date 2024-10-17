package com.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class RestAPISourceTest {

    private RestAPISource restAPISource;
    private final Logger logger = LogManager.getLogger(this.getClass());

    @BeforeEach
    public void setup() {
        // Replace with a valid API URL for testing purposes
        String apiURL = "https://jsonplaceholder.typicode.com/posts/1";
        String apiMethod = "GET";
        String headers = null; // Optional headers if needed

        restAPISource = new RestAPISource(apiURL, apiMethod, headers);
    }

    @Test
    public void testFetchDataFromAPI() {
        InputStream responseStream = restAPISource.fetchDataFromAPI();
        
        Assertions.assertNotNull(responseStream, "The response stream from the API should not be null");

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(responseStream))) {
            String line;
            int lineCount = 0;
            while ((line = reader.readLine()) != null && lineCount < 5) { 
                logger.info("API Response Line: " + line);
                lineCount++;
            }
            // Assert that we read at least one line from the InputStream
            Assertions.assertTrue(lineCount > 0, "The InputStream should have returned some data.");
        } catch (IOException e) {
            logger.error("Error while reading the InputStream from the API", e);
            Assertions.fail("Exception occurred during reading InputStream from the API");
        }
    }
}