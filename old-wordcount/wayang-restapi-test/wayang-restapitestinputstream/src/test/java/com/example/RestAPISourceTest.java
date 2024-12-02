package com.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.java.Java;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

public class RestAPISourceTest {

    private RestAPISource restAPISource;
    private final Logger logger = LogManager.getLogger(this.getClass());

    @BeforeEach
    public void setup() {
        String apiURL = "https://jsonplaceholder.typicode.com/posts/1";
        String apiMethod = "GET";
        String headers = null;

        restAPISource = new RestAPISource(apiURL, apiMethod, headers);
    }

    @Test
    public void testWordCountFromAPI() {
        InputStream responseStream = restAPISource.fetchDataFromAPI();

        Assertions.assertNotNull(responseStream, "The response stream from the API should not be null");

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(responseStream))) {
            String apiData = reader.lines().collect(Collectors.joining("\n"));
            logger.info("Fetched API Data: " + apiData);

            WayangContext wayangContext = new WayangContext(new Configuration())
                    .withPlugin(Java.basicPlugin());
            JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                    .withJobName("WordCount from REST API");

            Collection<Tuple2<String, Integer>> wordCounts = planBuilder
                    .loadCollection(Arrays.asList(apiData.split("\\W+"))).withName("Load API data")

                    .filter(token -> !token.isEmpty()).withName("Filter empty words")

                    .map(word -> new Tuple2<>(word.toLowerCase(), 1)).withName("Map words to count")

                    .reduceByKey(
                            Tuple2::getField0,
                            (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                    ).withName("Reduce by word")
                    .collect();

            Assertions.assertFalse(wordCounts.isEmpty(), "The word count result should not be empty");
            wordCounts.forEach(wordCount -> logger.info("Word: " + wordCount.getField0() + ", Count: " + wordCount.getField1()));

            int totalWordCount = wordCounts.stream().mapToInt(Tuple2::getField1).sum();
            logger.info("Total Word Count: " + totalWordCount);
            Assertions.assertTrue(totalWordCount > 0, "Word count should be more than 0");

        } catch (Exception e) {
            logger.error("Error while processing the API response: " + e.getMessage(), e);
            Assertions.fail("Exception occurred during the word count process");
        }
    }
}