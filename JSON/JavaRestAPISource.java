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

import org.apache.wayang.basic.operators.RestAPISource;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.core.util.Tuple;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

public class JavaRestAPISource extends RestAPISource implements JavaExecutionOperator {

    private static final Logger logger = LoggerFactory.getLogger(JavaRestAPISource.class);

    public JavaRestAPISource(RestAPISource restAPISource) {
        super(restAPISource.getAPIURL(), restAPISource.getAPIMethod(), restAPISource.getHeaders());
    }

    public JavaRestAPISource(String apiURL, String apiMethod, String headers) {
    super(apiURL, apiMethod, headers);
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
            JSONArray apiResponse = this.fetchDataFromAPI();
            Stream<String> responseStream = apiResponse.toList().stream().map(Object::toString);
            ((StreamChannel.Instance) outputs[0]).accept(responseStream);
    
            logger.info("Successfully streamed data from REST API: {}", this.getAPIURL());
    
        } catch (Exception e) {
            logger.error("Failed to fetch data from REST API at {}", this.getAPIURL(), e);
            throw new WayangException("Failed to fetch data from REST API.", e);
        }
    
        // Return an empty lineage and the output channel
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
