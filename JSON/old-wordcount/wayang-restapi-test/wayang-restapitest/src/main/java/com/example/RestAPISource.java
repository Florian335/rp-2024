package com.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.types.DataSetType;
import org.json.JSONArray;
import org.json.JSONException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class RestAPISource extends UnarySource<JSONArray> {
    private final Logger logger = LogManager.getLogger(this.getClass());
    private final String apiURL;
    private final String apiMethod;
    private final String headers;

    public RestAPISource(String apiURL, String apiMethod, String headers){
        super(DataSetType.createDefault(JSONArray.class));
        this.apiURL = apiURL;
        this.apiMethod = apiMethod;
        this.headers = headers;
    }

    public String getAPIURL() {
        return this.apiURL;
    }

    public String getAPIMethod() {
        return this.apiMethod;
    }

    public String getHeaders() {
        return this.headers;
    }

    public JSONArray fetchDataFromAPI() {
        HttpURLConnection connection = null;
        try {
            URL url = new URL(this.apiURL);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod(this.apiMethod);

            if (this.headers != null) {
                for (String header : this.headers.split(";")) {
                    String[] headerParts = header.split(":");
                    connection.setRequestProperty(headerParts[0].trim(), headerParts[1].trim());
                }
            }

            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            StringBuilder content = new StringBuilder();
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine).append("\n");
            }
            in.close();

            // Parse the response into a JSONArray
            return new JSONArray(content.toString());

        } catch (IOException | JSONException e) {
            this.logger.error("Unable to fetch data from REST API", e);
            return null;
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

}