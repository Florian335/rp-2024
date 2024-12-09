package org.apache.wayang.apps.pipelines;

import org.apache.wayang.basic.data.Tuple2;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Properties;

class ForecastResultPOSTTEST {
    private final float totalFTEs;
    private final int capacity;

    public ForecastResultPOSTTEST(float totalFTEs, int capacity) {
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

public class StandaloneAPIPOST {

    private static final Logger logger = LoggerFactory.getLogger(StandaloneAPIPOST.class);
    private static final String LOG_FILE_PATH_LATENCY = "post-api-latency.json";
    private static final String LOG_FILE_PATH_QUERIES = "post-queries-performance.json";

    private String apiURL;
    private String apiMethod;
    private String headers;
    private String payload;
    private static String forecastUser;
    private static String forecastToken;
    private static String hubspotToken;
        
    
    
        public StandaloneAPIPOST(String apiURL, String apiMethod, String headers, String payload) {
            this.apiURL = apiURL;
            this.apiMethod = apiMethod;
            this.headers = headers;
            this.payload = payload;
        }

        private static void appendlogtofile(JSONObject logrecord, String filePath) throws IOException {
            JSONArray existinglogs;

            if (Files.exists(Paths.get(filePath))) {
                String content = new String(Files.readAllBytes(Paths.get(filePath)));
                if (!content.isEmpty()) {
                    try {
                        existinglogs = new JSONArray(content);
                    } catch (JSONException e) {
                        logger.error("Corrupted JSON file content. Starting fresh for file: {}", filePath);
                        existinglogs = new JSONArray();
                    }
                } else {
                    existinglogs = new JSONArray();
                }
            } else {
                existinglogs = new JSONArray();
            }

            existinglogs.put(logrecord);
            Files.write(
                    Paths.get(filePath),
                    existinglogs.toString(4).getBytes(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING
            );
        }

        public static void logtoJSON(String stepname, Double latencyseconds, Double executiontime, String apiurl, String filePath) {
            try {
                JSONObject logrecord = new JSONObject();
                logrecord.put("timestamp", Instant.now().toString());
                logrecord.put("step", stepname);
                if (latencyseconds != null) {
                    logrecord.put("latency_seconds", latencyseconds);
                }
                if (executiontime != null) {
                    logrecord.put("execution_time_seconds", executiontime);
                }
                logrecord.put("url", apiurl != null ? apiurl : "Unknown");

                appendlogtofile(logrecord, filePath);
            } catch (Exception e) {
                logger.error("Unable to add data to JSON: {}", e.getMessage(), e);
            }
        }

        public static void logQueryTime(long starttime, long endtime, String stepname, String apiurl) {
            Double executiontimeseconds = (endtime - starttime) / 1000.0;
            logtoJSON(stepname, null, executiontimeseconds, apiurl, LOG_FILE_PATH_QUERIES);
        }

        public static void logAPIlatency(long starttime, long endtime, String stepname, String apiurl) {
            Double latencyseconds = (endtime - starttime) / 1000.0;
            logtoJSON(stepname, latencyseconds, null, apiurl, LOG_FILE_PATH_LATENCY);
        }
    
        public JSONArray fetchDataFromAPI() {
            String hardcodedURL = "https://api.hubapi.com/crm/v3/objects/deals/search";
            logger.info("Fetching data from API with method: {}", this.apiMethod);
    
            long apistarttime = System.currentTimeMillis();
            HttpURLConnection connection = null;
            try {
                if ("POST".equalsIgnoreCase(this.apiMethod) && !this.apiURL.startsWith(hardcodedURL)) {
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
                    if (this.payload == null || this.payload.isEmpty()) {
                        logger.warn("No payload provided for POST request.");
                    } else {
                        try (OutputStream os = connection.getOutputStream()) {
                            byte[] input = this.payload.getBytes("utf-8");
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
                long apiendtime = System.currentTimeMillis();
                logAPIlatency(apistarttime, apiendtime, "API Latency", this.apiURL);
    
                try {
                    logger.info("Attempting to parse response as JSONArray.");
                    return new JSONArray(response);
                } catch (JSONException e) {
                    logger.info("Response is not a JSONArray. Trying as JSONObject.");
                }
    
                try {
                    JSONObject jsonObject = new JSONObject(response);
                    JSONArray jsonArray = new JSONArray();
                    jsonArray.put(jsonObject);
                    return jsonArray;
                } catch (JSONException e) {
                    logger.info("Response is not a JSONObject. Trying as CSV string.");
                }
    
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
                    logger.error("Missing required properties in configuration file.");
                    return;
                }
            } catch (IOException e) {
                logger.error("Error loading configuration file: {}", e.getMessage(), e);
                return;
            }

            String urlForecast = String.format(
                    "https://api.forecastapp.com/aggregate/project_export?timeframe_type=monthly&timeframe=custom&starting=%s&ending=%s",
                    monthToday, month2m
            );

            String urlHubspot = "https://api.hubapi.com/crm/v3/objects/deals/search";
        
        try {
            long starttime_f = System.currentTimeMillis();
            ForecastResultPOSTTEST forecastResult = ForecastPipelinePOSTTEST(urlForecast);
            long endtime_f = System.currentTimeMillis();
            logQueryTime(starttime_f, endtime_f, "Forecast Entire Query Process",null);


            long starttime_h = System.currentTimeMillis();
            double totalFTEsHubspot = HubspotPipelinePOSTTEST(urlHubspot, monthToday);
            long endtime_h = System.currentTimeMillis();
            logQueryTime(starttime_h, endtime_h, "Hubspot Entire Query Process",null);


            logger.info("Pipeline FTEs: {}", totalFTEsHubspot);

            int capacity = forecastResult.getCapacity();
            logger.info("Capacity: {}", capacity);
            float totalFTEsForecast = forecastResult.getTotalFTEs();
            logger.info("Committed FTEs: {}", totalFTEsForecast);

            double remainingCapacity = capacity - (totalFTEsHubspot + totalFTEsForecast);

            if (remainingCapacity < 0) {
                logger.warn("Over capacity! Remaining capacity: {}", remainingCapacity);
            } else {
                logger.info("Enough capacity. Remaining capacity: {}", remainingCapacity);
            }
        } catch (Exception e) {
            logger.error("Error during Forecast API processing: {}", e.getMessage(), e);
        }
    }
    private static long toEpochMilliseconds(String dateString) {
        LocalDate date = LocalDate.parse(dateString, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        return date.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
    }

    private static ForecastResultPOSTTEST ForecastPipelinePOSTTEST(String urlForecast) {
        String apiMethod = "GET";
        String headers = String.format("User-Agent: %s; Authorization: Bearer %s; Forecast-Account-ID: %s", forecastUser, forecastToken, forecastUser);
        String payload = null;
        logger.info("Fetching data from {}", urlForecast);
    
        float totalFTEs = 0.0f;
        int capacity = 0;
    
        try {
            StandaloneAPIPOST api = new StandaloneAPIPOST(urlForecast, apiMethod, headers, payload);
            JSONArray response = api.fetchDataFromAPI();
    
            List<String> allowedRoles = Arrays.asList("DK", "US inc.");
            List<Tuple2<Float, String>> filteredData = new ArrayList<>();

            long starttime_f = System.currentTimeMillis();
            for (int i = 0; i < response.length(); i++) {
                JSONObject json = response.getJSONObject(i);
                if (allowedRoles.contains(json.optString("Roles", ""))) {
                    String jan2025str = json.optString("Jan 2025", "0");
                    float fte;
                    try {
                        fte = Float.parseFloat(jan2025str) / (float) 172.5;
                    } catch (NumberFormatException e) {
                        logger.error("Invalid number for January 2025: " + jan2025str, e);
                        fte = 0.0f;
                    }
                    String person = json.optString("Person", "Unknown");
                    filteredData.add(new Tuple2<>(fte, person));
                }
            }
    
            totalFTEs = filteredData.stream()
                    .map(tuple -> tuple.field0)
                    .reduce(0.0f, Float::sum);
    
            capacity = (int) filteredData.stream()
                    .map(tuple -> tuple.field1)
                    .distinct()
                    .count();
            
            long endtime_f = System.currentTimeMillis();
            logQueryTime(starttime_f, endtime_f, "Forecast Query",null);
    
        } catch (Exception e) {
            logger.error("Error fetching data from Forecast API: {}", e.getMessage(), e);
        }
    
        return new ForecastResultPOSTTEST(totalFTEs, capacity);
    }
    

    private static double HubspotPipelinePOSTTEST(String urlHubspot, String monthToday) {
        String apiMethod = "POST";
        String headers = String.format("accept: application/json; content-type: application/json; authorization: Bearer %s", hubspotToken);
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
    
                StandaloneAPIPOST api = new StandaloneAPIPOST(urlHubspot, apiMethod, headers, payload);
                JSONArray rawResponse = api.fetchDataFromAPI();

                for (int i = 0; i < rawResponse.length(); i++) {
                    JSONObject jsonObject = rawResponse.getJSONObject(i);
                    if (jsonObject.has("results")) {
                        JSONArray resultsArray = jsonObject.getJSONArray("results");
                        for (int j = 0; j < resultsArray.length(); j++) {
                            JSONObject resultObject = resultsArray.optJSONObject(j);
                            if (resultObject != null && resultObject.has("properties")) {
                                allProperties.add(resultObject.getJSONObject("properties"));
                            }
                        }
                    }
                }
    
                after = extractAfterToken(rawResponse);
                if (after == null || after.isEmpty()) {
                    moreResults = false;
                }
            }
    
            long starttime_h = System.currentTimeMillis();
            for (JSONObject obj : allProperties) {
                if (obj.has("start_date") && !obj.isNull("start_date") &&
                            obj.has("end_date") && !obj.isNull("end_date") &&
                            obj.has("fte_s_") && !obj.isNull("fte_s_")) {
                    LocalDate startDate = LocalDate.parse(obj.getString("start_date"), DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                    LocalDate endDate = LocalDate.parse(obj.getString("end_date"), DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    
                    if (YearMonth.from(startDate).equals(filterMonth)) {
                        long monthsBetween = Math.max(1, ChronoUnit.MONTHS.between(startDate, endDate));
                        double fteValue = obj.getDouble("fte_s_");
                        totalFTEs += fteValue / monthsBetween;
                    }
                }
            }
            long endtime_h = System.currentTimeMillis();
            logQueryTime(starttime_h, endtime_h, "HubSpot Query",null);
        } catch (Exception e) {
            logger.error("Error fetching data from HubSpot API: {}", e.getMessage(), e);
        }
    
        return totalFTEs;
    }
    

    private static String extractAfterToken(JSONArray rawResponse) {
        for (int i = 0; i < rawResponse.length(); i++) {
            JSONObject jsonObject = rawResponse.optJSONObject(i);
            if (jsonObject != null && jsonObject.has("paging")) {
                JSONObject paging = jsonObject.getJSONObject("paging");
                if (paging.has("next")) {
                    JSONObject next = paging.getJSONObject("next");
                    return next.optString("after", null);
                }
            }
        }
        return null;
    }
}