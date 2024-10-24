package com.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.types.DataSetType;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class RestAPISource extends UnarySource<InputStream> {
    private final Logger logger = LogManager.getLogger(this.getClass());
    private final String apiURL;
    private final String apiMethod;
    private final String headers;

    public RestAPISource(String apiURL, String apiMethod, String headers){
        super(DataSetType.createDefault(InputStream.class));
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

    public InputStream fetchDataFromAPI() {
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
    
            int responseCode = connection.getResponseCode(); 
            logger.info("Response Code: " + responseCode);
    
            if (responseCode != HttpURLConnection.HTTP_OK) {
                logger.error("Failed to fetch data: HTTP response code " + responseCode);
                return null;
            }
    
            logger.info("Successfully fetched data from REST API.");
    
            return connection.getInputStream();
    
        } catch (IOException e) {
            this.logger.error("Unable to fetch data from REST API", e);
            return null;
        } 
    }
}    