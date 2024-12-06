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

package org.apache.wayang.java.operators;

import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.core.util.Tuple;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.stream.Stream;

public class JavaRestAPISourceTest {

    private static final String TEST_API_URL = "https://jsonplaceholder.typicode.com/posts/1";
    private static final String TEST_API_METHOD = "GET";
    private static final String TEST_HEADERS = "";

    @Test
    public void testEvaluate() {
        // Initialize the JavaRestAPISource with a test URL
        JavaRestAPISource restAPISource = new JavaRestAPISource(TEST_API_URL, TEST_API_METHOD, TEST_HEADERS);
        
        // Mock the necessary objects
        StreamChannel.Instance outputChannelInstance = Mockito.mock(StreamChannel.Instance.class);
        ChannelInstance[] inputs = new ChannelInstance[0];
        ChannelInstance[] outputs = new ChannelInstance[]{outputChannelInstance};
        JavaExecutor javaExecutor = Mockito.mock(JavaExecutor.class);
        OptimizationContext.OperatorContext operatorContext = Mockito.mock(OptimizationContext.OperatorContext.class);
        
        // Call the evaluate method
        Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> result = restAPISource.evaluate(inputs, outputs, javaExecutor, operatorContext);
        
        // Verify the output stream was accepted
        Mockito.verify(outputChannelInstance, Mockito.times(1)).accept(Mockito.any(Stream.class));

        // Ensure no exceptions were thrown
        Assert.assertNotNull("Execution lineage nodes should not be null", result.field0);
        Assert.assertNotNull("Output channels should not be null", result.field1);
        Assert.assertTrue("Output channels should contain the output instance", result.field1.contains(outputChannelInstance));
    }

    @Test
    public void testFetchDataFromAPI() throws Exception {
        JavaRestAPISource restAPISource = new JavaRestAPISource(TEST_API_URL, TEST_API_METHOD, TEST_HEADERS);

        JSONArray jsonResponse = restAPISource.fetchDataFromAPI();
        
        Assert.assertNotNull("The JSON response should not be null", jsonResponse);
        Assert.assertTrue("The JSON response should contain data", jsonResponse.length() > 0);
        
        for (int i = 0; i < jsonResponse.length(); i++) {
            JSONObject post = jsonResponse.getJSONObject(i);
            Assert.assertTrue("Each post should have a userId", post.has("userId"));
            Assert.assertTrue("Each post should have an id", post.has("id"));
            Assert.assertTrue("Each post should have a title", post.has("title"));
            Assert.assertTrue("Each post should have a body", post.has("body"));
        }
    }
}
