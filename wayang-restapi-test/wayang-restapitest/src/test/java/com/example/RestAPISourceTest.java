package com.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RestAPISourceTest {

    private RestAPISource restAPISource;
    private final Logger logger = LogManager.getLogger(this.getClass());

    @BeforeEach
    public void setup() {
        // Use an API endpoint that returns a JSON array
        String apiURL = "https://jsonplaceholder.typicode.com/posts";
        String apiMethod = "GET";
        String headers = null; // Optional headers if needed

        restAPISource = new RestAPISource(apiURL, apiMethod, headers);
    }

    @Test
    public void testFetchDataFromAPI() {
        JSONArray response = restAPISource.fetchDataFromAPI();
        
        Assertions.assertNotNull(response, "The response from the API should not be null");

        logger.info("API Response: " + response.toString());

        Assertions.assertTrue(response.length() > 0, "The response should not be empty");

        JSONObject firstElement = response.getJSONObject(0);
        logger.info("First API Response: " + firstElement.toString());

        Assertions.assertTrue(firstElement.has("userId"), "The first element should have a 'userId' field");
        Assertions.assertTrue(firstElement.has("id"), "The first element should have an 'id' field");
        Assertions.assertTrue(firstElement.has("title"), "The first element should have a 'title' field");
    }
}
