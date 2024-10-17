package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
        // Fetch the data from the API
        JsonNode response = restAPISource.fetchDataFromAPI();
        
        // Assert that the response is not null
        Assertions.assertNotNull(response, "The response from the API should not be null");

        // Log the response
        logger.info("API Response: " + response.toString());

        // Add some validation depending on your expected JSON structure
        Assertions.assertTrue(response.has("userId"), "The response should have a 'userId' field");
        Assertions.assertTrue(response.has("id"), "The response should have an 'id' field");
        Assertions.assertTrue(response.has("title"), "The response should have a 'title' field");
    }
}