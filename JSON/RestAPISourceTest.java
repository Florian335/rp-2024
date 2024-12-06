package org.apache.wayang.basic.operators;

// import org.apache.wayang.basic.operators.RestAPISource;
// import org.apache.wayang.core.api.WayangContext;
import org.json.JSONArray;
// import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

public class RestAPISourceTest {

    @Test
    public void testFetchDataFromJsonPlaceholderAPI() {
        // Define API details for JSONPlaceholder
        String apiUrl = "https://jsonplaceholder.typicode.com/posts";
        String apiMethod = "GET";
        String headers = ""; // No headers needed for JSONPlaceholder

        // Create the RestAPISource
        RestAPISource restAPISource = new RestAPISource(apiUrl, apiMethod, headers);

        // Fetch data from the API
        JSONArray responseData = restAPISource.fetchDataFromAPI();

        // Validate the response
        Assert.assertNotNull("The response should not be null.", responseData);
        Assert.assertTrue("The response should be a JSONArray.", responseData instanceof JSONArray);
        Assert.assertTrue("The response should contain at least one object.", responseData.length() > 0);

        // // Print the first 10 entries to the console
        // int entriesToPrint = Math.min(10, responseData.length());
        // for (int i = 0; i < entriesToPrint; i++) {
        //     JSONObject object = responseData.getJSONObject(i);
        //     System.out.println("Entry " + (i + 1) + ": " + object.toString());
        // }
    }

    @Test
    public void testCachedResponse() {
        // Define API details
        String apiUrl = "https://jsonplaceholder.typicode.com/posts";
        String apiMethod = "GET";
        String headers = ""; // No headers needed

        // Create the RestAPISource
        RestAPISource restAPISource = new RestAPISource(apiUrl, apiMethod, headers);

        // Fetch data the first time
        JSONArray firstResponse = restAPISource.fetchDataFromAPI();

        // Fetch data the second time (should use cache)
        JSONArray cachedResponse = restAPISource.fetchDataFromAPI();

        // Verify that the cached response is the same as the first response
        Assert.assertSame("The cached response should be the same instance as the first response.", firstResponse, cachedResponse);
    }
}
