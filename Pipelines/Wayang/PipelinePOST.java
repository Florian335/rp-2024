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
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.wayang.apps.pipelines;

import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.java.Java;
import org.apache.wayang.api.DataQuanta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.time.temporal.ChronoUnit;
import java.time.LocalDate;
import java.time.YearMonth;
import java.util.stream.Collectors;
import java.time.ZoneOffset;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Date;



class ForecastResultPOST {
    private final float totalFTEs;
    private final int capacity;

    public ForecastResultPOST(float totalFTEs, int capacity) {
        this.totalFTEs = totalFTEs;
        this.capacity = capacity;
    }

    public float getTotalFTEs() {
        return totalFTEs;
    }

    public int getCapacity() {
        return capacity;
    }
}

public class PipelinePOST {

    private static final Logger log = LoggerFactory.getLogger(PipelinePOST.class);

    private static String forecastUser;
    private static String forecastToken;
    private static String hubspotToken;
    private static final String LOG_FILE_PATH = "post-queries-performance.json";

    public static void logtoJSON(String stepname, Double latencyseconds, Double executiontime){
        try {
            JSONObject logrecord = new JSONObject();
            logrecord.put("timestamp", Date.from(Instant.now()).toString());
            logrecord.put("step",stepname);
            logrecord.put("execution_time_seconds", executiontime);

            appendlogtofile(logrecord);
        } catch (Exception e) {
            log.error("Unable to add data to JSON: {}", e.getMessage(),e);
        }
    }

    public static void logQueryTime(long starttime, long endtime, String stepname){
        Double executiontimeseconds = (endtime - starttime) / 1000.0;
        logtoJSON(stepname, null, executiontimeseconds);
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

    public static void main(String[] args) {
        Properties properties = new Properties();
        String configFilePath = "/home/flo/incubator-wayang/wayang-benchmark/src/main/java/org/apache/wayang/apps/wordcount/config.properties";

        LocalDateTime today = LocalDateTime.now()
                .with(TemporalAdjusters.firstDayOfNextMonth())
                .withHour(0)
                .withMinute(0)
                .withSecond(0)
                .withNano(0);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String monthToday = today.format(formatter);

        LocalDateTime futureDate = today.plus(2, ChronoUnit.MONTHS);
        String month2m = futureDate.format(formatter);

        try (FileInputStream fis = new FileInputStream(configFilePath)) {
            properties.load(fis);
            forecastUser = properties.getProperty("Forecast_user_agent");
            forecastToken = properties.getProperty("Forecast");
            hubspotToken = properties.getProperty("Hubspot");

            if (forecastUser == null || forecastToken == null || hubspotToken == null) {
                log.error("Missing required properties in configuration file.");
                return;
            }
        } catch (IOException e) {
            log.error("Error loading configuration file: {}", e.getMessage(), e);
            return;
        }

        WayangContext wayangContext = new WayangContext(new Configuration())
                .withPlugin(Java.basicPlugin());
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("Pipelines")
                .withUdfJarOf(PipelinePOST.class);

        String urlForecast = String.format(
                "https://api.forecastapp.com/aggregate/project_export?timeframe_type=monthly&timeframe=custom&starting=%s&ending=%s",
                monthToday, month2m
        );

        String urlHubspot = "https://api.hubapi.com/crm/v3/objects/deals/search";
        
        try {
            long starttime_f = System.currentTimeMillis();
            ForecastResultPOST forecastResult = ForecastPipelinePOST(planBuilder, urlForecast);
            long endtime_f = System.currentTimeMillis();
            logQueryTime(starttime_f, endtime_f, "Forecast Entire Query Process");

            long starttime_h = System.currentTimeMillis();
            double totalFTEsHubspot = HubspotPipelinePOST(planBuilder, urlHubspot, monthToday);
            long endtime_h = System.currentTimeMillis();
            logQueryTime(starttime_h, endtime_h, "HubSpot Entire Query Process");



            log.info("Pipeline FTEs: {}", totalFTEsHubspot);

            int capacity = forecastResult.getCapacity();
            log.info("Capacity: {}", capacity);
            float totalFTEsForecast = forecastResult.getTotalFTEs();
            log.info("Committed FTEs: {}", totalFTEsForecast);

            double remainingCapacity = capacity - (totalFTEsHubspot + totalFTEsForecast);

            if (remainingCapacity < 0) {
                log.warn("Over capacity! Remaining capacity: {}", remainingCapacity);
            } else {
                log.info("Enough capacity. Remaining capacity: {}", remainingCapacity);
            }
        } catch (Exception e) {
            log.error("Error during Forecast API processing: {}", e.getMessage(), e);
        }
    }

    private static long toEpochMilliseconds(String dateString) {
        LocalDate date = LocalDate.parse(dateString, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        return date.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
    }

    private static ForecastResultPOST ForecastPipelinePOST(JavaPlanBuilder planBuilder, String urlForecast) {
        String apiMethod = "GET";
        String headers = String.format("User-Agent: %s; Authorization: Bearer %s; Forecast-Account-ID: %s", forecastUser, forecastToken, forecastUser);
        String payload = null;
        log.info("Fetching data from {}", urlForecast);

        float totalFTEs = 0.0f;
        int capacity = 0;

        try {
            List<String> allowedRoles = Arrays.asList("DK", "US inc.");

            long starttime_f = System.currentTimeMillis();
            Collection<Tuple2<Float,String>> filteredData = planBuilder
                .readRestAPISource(urlForecast, apiMethod, headers, payload) 
                .filter(json -> allowedRoles.contains(json.optString("Roles", "")))  
                .map(json -> {
                    String jan2025str = json.optString("Jan 2025", "0");
                    float fte;
                    try {
                        fte = Float.parseFloat(jan2025str) / (float) 172.5;
                    } catch (NumberFormatException e) {
                        log.error("Invalid number for January 2025: " + jan2025str, e);
                        fte = 0.0f;
                    }
                    String person = json.optString("Person", "Unknown");
                    return new Tuple2<>(fte, person);
                })
                .collect();  

            totalFTEs = filteredData.stream()
                .map(tuple -> tuple.field0)  
                .reduce(0.0f, Float::sum);

            capacity = (int) filteredData.stream()
                .map(tuple -> tuple.field1)  
                .distinct()
                .count();
            
            long endtime_f = System.currentTimeMillis();
            logQueryTime(starttime_f, endtime_f, "Forecast Query");


        } catch (Exception e) {
            log.error("Error fetching data from Forecast API: {}", e.getMessage(), e);
        }

        return new ForecastResultPOST(totalFTEs, capacity);
    }

    private static double HubspotPipelinePOST(JavaPlanBuilder planBuilder, String urlHubspot, String monthToday) {
        String apiMethod = "POST";
        String headers = String.format("accept: application/json; content-type: application/json; authorization: Bearer %s", hubspotToken);
        String currentUrlHubspot = urlHubspot;
        boolean moreResults = true;
        YearMonth filterMonth = YearMonth.from(LocalDate.parse(monthToday, DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        double totalFTEs = 0.0;
        long startOfMonthEpoch = toEpochMilliseconds(filterMonth.atDay(1).toString());
        long endOfMonthEpoch = toEpochMilliseconds(filterMonth.atEndOfMonth().toString());
        String payloadTemplate = "{" +
            "  \"filterGroups\": [" +
            "    {" +
            "      \"filters\": [" +
            "        {" +
            "          \"propertyName\": \"start_date\"," +
            "          \"operator\": \"BETWEEN\"," +
            "          \"value\": \"%d\"," +
            "          \"highValue\": \"%d\"" +
            "        }" +
            "      ]" +
            "    }" +
            "  ]," +
            "  \"properties\": [\"start_date\", \"end_date\", \"fte_s_\"], " +
            "  \"after\": \"%s\"" +
            "}";

        Collection<JSONObject> allProperties = new ArrayList<>();
        String after = null;

        try {
            while (moreResults) {
                String payload = after == null
                    ? String.format(payloadTemplate, startOfMonthEpoch, endOfMonthEpoch, "")
                    : String.format(payloadTemplate, startOfMonthEpoch, endOfMonthEpoch, after);

                Collection<JSONObject> rawResponse = planBuilder
                        .readRestAPISource(currentUrlHubspot, apiMethod, headers, payload)
                        .collect();

                for (JSONObject jsonObject : rawResponse) {
                    if (jsonObject.has("results")) {
                        JSONArray resultsArray = jsonObject.getJSONArray("results");

                        for (int i = 0; i < resultsArray.length(); i++) {
                            JSONObject resultObject = resultsArray.optJSONObject(i);
                            if (resultObject != null && resultObject.has("properties")) {
                                allProperties.add(resultObject.getJSONObject("properties"));
                            }
                        }
                    }
                }

                log.info("Fetched {} responses in this page", rawResponse.size());

                // Extract pagination token
                after = extractAfterToken(rawResponse);
                if (after == null || after.isEmpty()) {
                    moreResults = false;
                } else {
                }
            }


            long starttime_h = System.currentTimeMillis();
            Collection<Double> fteCollection = planBuilder
                .loadCollection(allProperties) 
                .filter(obj -> obj.has("start_date") && !obj.isNull("start_date") &&
                            obj.has("end_date") && !obj.isNull("end_date") &&
                            obj.has("fte_s_") && !obj.isNull("fte_s_")) 
                .map(obj -> {
                    try {
                        LocalDate startDate = LocalDate.parse(obj.getString("start_date"), DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                        LocalDate endDate = LocalDate.parse(obj.getString("end_date"), DateTimeFormatter.ofPattern("yyyy-MM-dd"));

                        if (YearMonth.from(startDate).equals(filterMonth)) { 
                            long monthsBetween = Math.max(1, ChronoUnit.MONTHS.between(startDate, endDate)); 
                            double fteValue = obj.getDouble("fte_s_");
                            return fteValue / monthsBetween;
                        }
                    } catch (Exception e) {
                        log.warn("Skipping deal due to parsing error: {}", obj, e);
                    }
                    return 0.0; 
                })
                .reduce((fte1, fte2) -> fte1 + fte2) 
                .collect(); 
                
            totalFTEs = fteCollection.isEmpty() ? 0.0 : fteCollection.iterator().next();

            long endtime_h = System.currentTimeMillis();
            logQueryTime(starttime_h, endtime_h, "HubSpot Query");

        } catch (Exception e) {
            log.error("Error fetching data from HubSpot API: {}", e.getMessage(), e);
        }

        return totalFTEs; 
        }

    private static String extractAfterToken(Collection<JSONObject> rawResponse) {
        for (JSONObject jsonObject : rawResponse) {
            if (jsonObject.has("paging")) {  
                JSONObject paging = jsonObject.getJSONObject("paging");
                if (paging.has("next")) {    
                    JSONObject next = paging.getJSONObject("next");
                    return next.getString("after"); 
                }
            }
        }
        return null; 
    }
}
