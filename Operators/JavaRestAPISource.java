/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.wayang.java.operators;

import org.apache.wayang.basic.operators.RestAPISource;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.core.util.Tuple;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;    
import java.net.HttpURLConnection;
import java.net.URL;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Date;

public class JavaRestAPISource extends RestAPISource implements JavaExecutionOperator {

    private static final Logger logger = LoggerFactory.getLogger(JavaRestAPISource.class);
    private static final String LOG_FILE_PATH = "json-api-latency.json";

    public static void logtoJSON(String stepname, Double latencyseconds, String apiurl) {
        try {
            JSONObject logrecord = new JSONObject();
            logrecord.put("timestamp", Date.from(Instant.now()).toString());
            logrecord.put("step", stepname);
            logrecord.put("latency_seconds", latencyseconds);
            logrecord.put("url", apiurl);

            appendlogtofile(logrecord);
        } catch (Exception e) {
            logger.error("Unable to add data to JSON: {}", e.getMessage(), e);
        }
    }

    public static void logAPIlatency(long starttime, long endtime, String stepname, String apiurl){
        Double latencyseconds = (endtime - starttime) / 1000.0;
        logtoJSON(stepname, latencyseconds, apiurl);
    }

    private static void appendlogtofile(JSONObject logrecord) throws IOException {
        JSONArray existinglogs;

        if (Files.exists(Paths.get(LOG_FILE_PATH))){
            String content = new String(Files.readAllBytes(Paths.get(LOG_FILE_PATH)));
            if (!content.isEmpty()){
                existinglogs = new JSONArray(content);
            } else {
                existinglogs = new JSONArray();
            }
        } else {
            existinglogs = new JSONArray();
        }

        existinglogs.put(logrecord);
        Files.write(
            Paths.get(LOG_FILE_PATH),
            existinglogs.toString(4).getBytes(),
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING
        );
    }


    public JavaRestAPISource(RestAPISource restAPISource) {
        super(restAPISource.getAPIURL(), restAPISource.getAPIMethod(), restAPISource.getHeaders(), restAPISource.getPayload());
    }

    public JavaRestAPISource(String apiURL, String apiMethod, String headers, String payload) {
        super(apiURL, apiMethod, headers, payload);
    }

    public JSONArray fetchDataFromAPI() {
        String hardcodedURL = "https://api.hubapi.com/crm/v3/objects/deals/search"; 
        logger.info("Fetching data from API with method: {}", this.apiMethod);

        long apistarttime = System.currentTimeMillis();
        HttpURLConnection connection = null;
        try {
            if ("POST".equalsIgnoreCase(this.apiMethod) && !hardcodedURL.equals(this.apiURL)) {
                logger.error("POST requests are only allowed to the hardcoded URL: {}", hardcodedURL);
                throw new IllegalArgumentException("POST requests must use the hardcoded URL.");
            }

            URL url = new URL(this.apiURL);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod(this.apiMethod); 

            if (!this.headers.isEmpty()) {
                for (String header : this.headers.split(";")) {
                    String[] headerParts = header.trim().split(":", 2);
                    if (headerParts.length == 2) {
                        connection.setRequestProperty(headerParts[0].trim(), headerParts[1].trim());
                    } else {
                        logger.warn("Invalid header format: {}", header);
                    }
                }
            }

            if ("POST".equalsIgnoreCase(this.apiMethod)) {
                connection.setDoOutput(true); 
                String payload = this.getPayload(); 
                if (payload == null || payload.isEmpty()) {
                    logger.warn("No payload provided for POST request.");
                } else {
                    try (OutputStream os = connection.getOutputStream()) {
                        byte[] input = payload.getBytes("utf-8");
                        os.write(input, 0, input.length);
                    }
                }
            }
            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            StringBuilder content = new StringBuilder();
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine).append("\n");
            }
            in.close();

            String response = content.toString();

            long apiendttime = System.currentTimeMillis();
            logAPIlatency(apistarttime, apiendttime, "API Latency",this.apiURL);

            try {
                logger.info("Attempting to parse response as JSONArray.");
                return new JSONArray(response);
            } catch (JSONException e) {
                logger.info("Response is not a JSONArray. Trying as JSONObject.");
            }

            // Attempt to parse as JSONObject
            try {
                JSONObject jsonObject = new JSONObject(response);
                JSONArray jsonArray = new JSONArray();
                jsonArray.put(jsonObject);
                return jsonArray;
            } catch (JSONException e) {
                logger.info("Response is not a JSONObject. Trying as CSV string.");
            }

            // Treat response as CSV and parse
            try {
                return convertCsvToJson(response);
            } catch (Exception e) {
                logger.error("Failed to parse response as CSV string.", e);
            }

        } catch (IOException e) {
            logger.error("Unable to fetch data from REST API", e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
        return new JSONArray();
    }


    private JSONArray convertCsvToJson(String dataString) {
        String[] lines = dataString.split("\n");

        if (lines.length == 0) {
            return new JSONArray();
        }

        String[] columns = lines[0].split(",");

        JSONArray jsonArray = new JSONArray();

        for (int i = 1; i < lines.length; i++) {
            if (lines[i].trim().isEmpty()) {
                continue;
            }

            String[] values = lines[i].split(",");
            JSONObject jsonObject = new JSONObject();

            for (int j = 0; j < columns.length; j++) {
                String columnName = columns[j].trim();
                String value = j < values.length ? values[j].trim() : "";
                jsonObject.put(columnName, value);
            }

            jsonArray.put(jsonObject);
        }

        return jsonArray;
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {
    
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();
    
        try {
            JSONArray apiResponse = fetchDataFromAPI();
            Stream<JSONObject> responseStream = IntStream.range(0, apiResponse.length())
                    .mapToObj(apiResponse::getJSONObject);
            ((StreamChannel.Instance) outputs[0]).accept(responseStream);

            logger.info("Successfully streamed data from REST API: {}", this.getAPIURL());
    
        } catch (Exception e) {
            logger.error("Failed to fetch data from REST API at {}", this.getAPIURL(), e);
            throw new WayangException("Failed to fetch data from REST API.", e);
        }
    
        return new Tuple<>(Collections.emptyList(), Arrays.asList(outputs));
    }

    @Override
    public JavaRestAPISource copy() {
        return new JavaRestAPISource(this);
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList("wayang.java.restapisource.load.prepare", "wayang.java.restapisource.load.main");
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        throw new UnsupportedOperationException(String.format("%s does not have input channels.", this));
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }
}
