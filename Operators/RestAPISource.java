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

package org.apache.wayang.basic.operators;

import org.apache.commons.lang3.Validate;
import org.apache.wayang.commons.util.profiledb.model.measurement.TimeMeasurement;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.types.DataSetType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Optional;
import java.util.OptionalDouble;

public class RestAPISource extends UnarySource<JSONObject> {
    private final Logger logger = LogManager.getLogger(this.getClass());
    public final String apiURL;
    public final String apiMethod;
    public final String headers;
    public final String payload; 


    public RestAPISource(String apiURL, String apiMethod, String headers, String payload) {
        super(DataSetType.createDefault(JSONObject.class));
        this.apiURL = apiURL;
        this.apiMethod = apiMethod;
        this.headers = headers;
        this.payload = payload;

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

    public String getPayload() {
        return this.payload;
    }
}

    // @Override
    // public Optional<org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator> createCardinalityEstimator(
    //         final int outputIndex,
    //         final Configuration configuration) {
    //     Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
    //     return Optional.of(new RestAPISource.CardinalityEstimator());
    // }

    // public class CardinalityEstimator implements org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator {

    //     public final CardinalityEstimate FALLBACK_ESTIMATE = new CardinalityEstimate(1000L, 100000000L, 0.7);

    //     public static final double CORRECTNESS_PROBABILITY = 0.95d;
    //     public static final double EXPECTED_ESTIMATE_DEVIATION = 0.05;

    //     @Override
    //     public CardinalityEstimate estimate(OptimizationContext optimizationContext, CardinalityEstimate... inputEstimates) {
    //         Validate.isTrue(RestAPISource.this.getNumInputs() == inputEstimates.length);

    //         final TimeMeasurement timeMeasurement = optimizationContext.getJob().getStopWatch().start(
    //             "Optimization", "Cardinality&Load Estimation", "Estimate source cardinalities"
    //         );

    //         String jobCacheKey = String.format("%s.estimate(%s)", this.getClass().getCanonicalName(), RestAPISource.this.apiURL);
    //         CardinalityEstimate cardinalityEstimate = optimizationContext.queryJobCache(jobCacheKey, CardinalityEstimate.class);
    //         if (cardinalityEstimate != null) return cardinalityEstimate;

    //         try {
    //             if (cachedResponse == null) {
    //                 fetchDataFromAPI();
    //             }
    //             long estimatedCount = cachedResponse.length();

    //             double expectedDeviation = estimatedCount * EXPECTED_ESTIMATE_DEVIATION;
    //             cardinalityEstimate = new CardinalityEstimate(
    //                     (long) (estimatedCount - expectedDeviation),
    //                     (long) (estimatedCount + expectedDeviation),
    //                     CORRECTNESS_PROBABILITY
    //             );

    //             optimizationContext.putIntoJobCache(jobCacheKey, cardinalityEstimate);

    //         } catch (Exception e) {
    //             RestAPISource.this.logger.warn("Failed to estimate cardinality for {}: using fallback estimate.", RestAPISource.this.apiURL, e);
    //             cardinalityEstimate = this.FALLBACK_ESTIMATE;
    //         }

    //         timeMeasurement.stop();
    //         return cardinalityEstimate;
    //     }
    // }

    // public OptionalDouble estimateBytesPerLine() {
    //     if (this.cachedResponse == null) {
    //         fetchDataFromAPI();
    //         this.logger.info("Pulled data again....");
    //     }

    //     String responseString = this.cachedResponse.toString();
    //     int numBytes = responseString.getBytes().length;
    //     this.logger.info("Numbytes in the response: {}", numBytes);
    //     long numLines = this.cachedResponse.length();
    //     this.logger.info("Response Length: {}",numLines);

    //     if (numLines == 0) {
    //         this.logger.warn("Could not find any line-like elements in {}.", this.apiURL);
    //         return OptionalDouble.empty();
    //     }

    //     return OptionalDouble.of((double) numBytes / numLines);
    // }
