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

package org.apache.wayang.core.optimizer.channels;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.optimizer.DefaultOptimizationContext;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.OptimizationUtils;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.Junction;
import org.apache.wayang.core.test.DummyExecutionOperator;
import org.apache.wayang.core.test.DummyExternalReusableChannel;
import org.apache.wayang.core.test.DummyNonReusableChannel;
import org.apache.wayang.core.test.DummyReusableChannel;
import org.apache.wayang.core.test.MockFactory;
import org.apache.wayang.core.util.WayangCollections;

import java.util.Arrays;
import java.util.function.Supplier;

/**
 * Test suite for {@link ChannelConversionGraph}.
 */
public class ChannelConversionGraphTest {

    private static DefaultChannelConversion reusableToNonReusableChannelConversion;

    private static DefaultChannelConversion nonReusableToReusableChannelConversion;

    private static DefaultChannelConversion reusableToExternalChannelConversion;

    private static DefaultChannelConversion nonReusableToExternalChannelConversion;

    private static DefaultChannelConversion externalToNonReusableChannelConversion;

    private static Job job;

    private static Configuration configuration;

    private static Supplier<ExecutionOperator> createDummyExecutionOperatorFactory(ChannelDescriptor channelDescriptor) {
        return () -> {
            ExecutionOperator execOp = new DummyExecutionOperator(1, 1, false);
            execOp.getSupportedOutputChannels(0).add(channelDescriptor);
            return execOp;
        };
    }

    @BeforeClass
    public static void initializeChannelConversions() {
        reusableToNonReusableChannelConversion = new DefaultChannelConversion(
                DummyReusableChannel.DESCRIPTOR,
                DummyNonReusableChannel.DESCRIPTOR,
                createDummyExecutionOperatorFactory(DummyNonReusableChannel.DESCRIPTOR)
        );
        nonReusableToReusableChannelConversion = new DefaultChannelConversion(
                DummyNonReusableChannel.DESCRIPTOR,
                DummyReusableChannel.DESCRIPTOR,
                createDummyExecutionOperatorFactory(DummyReusableChannel.DESCRIPTOR)
        );
        reusableToExternalChannelConversion = new DefaultChannelConversion(
                DummyReusableChannel.DESCRIPTOR,
                DummyExternalReusableChannel.DESCRIPTOR,
                createDummyExecutionOperatorFactory(DummyExternalReusableChannel.DESCRIPTOR)
        );
        nonReusableToExternalChannelConversion = new DefaultChannelConversion(
                DummyNonReusableChannel.DESCRIPTOR,
                DummyNonReusableChannel.DESCRIPTOR,
                createDummyExecutionOperatorFactory(DummyNonReusableChannel.DESCRIPTOR)
        );
        externalToNonReusableChannelConversion = new DefaultChannelConversion(
                DummyExternalReusableChannel.DESCRIPTOR,
                DummyNonReusableChannel.DESCRIPTOR,
                createDummyExecutionOperatorFactory(DummyNonReusableChannel.DESCRIPTOR)
        );
        configuration = new Configuration();
        job = MockFactory.createJob(configuration);

    }

    @Test
    public void findDirectConversion() throws Exception {
        ChannelConversionGraph channelConversionGraph = new ChannelConversionGraph(configuration);

        ExecutionOperator sourceOperator = new DummyExecutionOperator(0, 1, false);
        sourceOperator.getSupportedOutputChannels(0).add(DummyReusableChannel.DESCRIPTOR);

        ExecutionOperator destOperator0 = new DummyExecutionOperator(1, 1, false);
        destOperator0.getSupportedInputChannels(0).add(DummyReusableChannel.DESCRIPTOR);
        destOperator0.getSupportedInputChannels(0).add(DummyNonReusableChannel.DESCRIPTOR);

        ExecutionOperator destOperator1 = new DummyExecutionOperator(1, 1, false);
        destOperator1.getSupportedInputChannels(0).add(DummyReusableChannel.DESCRIPTOR);
        destOperator1.getSupportedInputChannels(0).add(DummyNonReusableChannel.DESCRIPTOR);

        final OptimizationContext optimizationContext = new DefaultOptimizationContext(job);
        optimizationContext.addOneTimeOperator(sourceOperator).setOutputCardinality(0, new CardinalityEstimate(1000, 10000, 0.8d));

        channelConversionGraph.findMinimumCostJunction(
                sourceOperator.getOutput(0),
                Arrays.asList(destOperator0.getInput(0), destOperator1.getInput(0)),
                optimizationContext,
                false
        );
    }

    @Test
    public void findIntricateConversion() throws Exception {
        ChannelConversionGraph channelConversionGraph = new ChannelConversionGraph(new Configuration());
        channelConversionGraph.add(reusableToNonReusableChannelConversion);
        channelConversionGraph.add(nonReusableToReusableChannelConversion);
        channelConversionGraph.add(reusableToExternalChannelConversion);
        channelConversionGraph.add(nonReusableToExternalChannelConversion);
        channelConversionGraph.add(externalToNonReusableChannelConversion);

        ExecutionOperator sourceOperator = new DummyExecutionOperator(0, 1, false);
        sourceOperator.getSupportedOutputChannels(0).add(DummyReusableChannel.DESCRIPTOR);

        ExecutionOperator destOperator0 = new DummyExecutionOperator(1, 1, false);
        destOperator0.getSupportedInputChannels(0).add(DummyNonReusableChannel.DESCRIPTOR);

        ExecutionOperator destOperator1 = new DummyExecutionOperator(1, 1, false);
        destOperator1.getSupportedInputChannels(0).add(DummyReusableChannel.DESCRIPTOR);

        final OptimizationContext optimizationContext = new DefaultOptimizationContext(job);
        optimizationContext.addOneTimeOperator(sourceOperator).setOutputCardinality(0, new CardinalityEstimate(1000, 10000, 0.8d));

        Junction junction = channelConversionGraph.findMinimumCostJunction(
                sourceOperator.getOutput(0),
                Arrays.asList(destOperator0.getInput(0), destOperator1.getInput(0)),
                optimizationContext,
                false
        );
    }

    @Test
    public void findIntricateConversion2() throws Exception {
        ChannelConversionGraph channelConversionGraph = new ChannelConversionGraph(new Configuration());
        channelConversionGraph.add(reusableToNonReusableChannelConversion);
        channelConversionGraph.add(nonReusableToReusableChannelConversion);
        channelConversionGraph.add(reusableToExternalChannelConversion);
        channelConversionGraph.add(nonReusableToExternalChannelConversion);
        channelConversionGraph.add(externalToNonReusableChannelConversion);

        ExecutionOperator sourceOperator = new DummyExecutionOperator(0, 1, false);
        sourceOperator.getSupportedOutputChannels(0).add(DummyReusableChannel.DESCRIPTOR);

        ExecutionOperator destOperator0 = new DummyExecutionOperator(1, 1, false);
        destOperator0.getSupportedInputChannels(0).add(DummyNonReusableChannel.DESCRIPTOR);

        ExecutionOperator destOperator1 = new DummyExecutionOperator(1, 1, false);
        destOperator1.getSupportedInputChannels(0).add(DummyExternalReusableChannel.DESCRIPTOR);

        final OptimizationContext optimizationContext = new DefaultOptimizationContext(job);
        optimizationContext.addOneTimeOperator(sourceOperator).setOutputCardinality(0, new CardinalityEstimate(1000, 10000, 0.8d));

        Junction junction = channelConversionGraph.findMinimumCostJunction(
                sourceOperator.getOutput(0),
                Arrays.asList(destOperator0.getInput(0), destOperator1.getInput(0)),
                optimizationContext,
                false
        );
    }

    @Test
    public void updateExistingConversionWithOnlySourceChannel() throws Exception {
        ChannelConversionGraph channelConversionGraph = new ChannelConversionGraph(new Configuration());
        channelConversionGraph.add(reusableToNonReusableChannelConversion);
        channelConversionGraph.add(nonReusableToReusableChannelConversion);
        channelConversionGraph.add(reusableToExternalChannelConversion);
        channelConversionGraph.add(nonReusableToExternalChannelConversion);
        channelConversionGraph.add(externalToNonReusableChannelConversion);

        ExecutionOperator sourceOperator = new DummyExecutionOperator(0, 1, false);
        sourceOperator.getSupportedOutputChannels(0).add(DummyNonReusableChannel.DESCRIPTOR);

        ExecutionOperator destOperator0 = new DummyExecutionOperator(1, 1, false);
        destOperator0.getSupportedInputChannels(0).add(DummyReusableChannel.DESCRIPTOR);

        ExecutionOperator destOperator1 = new DummyExecutionOperator(1, 1, false);
        destOperator1.getSupportedInputChannels(0).add(DummyExternalReusableChannel.DESCRIPTOR);

        final OptimizationContext optimizationContext = new DefaultOptimizationContext(job);
        optimizationContext.addOneTimeOperator(sourceOperator).setOutputCardinality(0, new CardinalityEstimate(1000, 10000, 0.8d));

        final Channel sourceChannel = DummyNonReusableChannel.DESCRIPTOR.createChannel(sourceOperator.getOutput(0), configuration);

        Junction junction = channelConversionGraph.findMinimumCostJunction(
                sourceOperator.getOutput(0),
                Arrays.asList(sourceChannel),
                Arrays.asList(destOperator0.getInput(0), destOperator1.getInput(0)),
                optimizationContext
        );

        Assert.assertTrue(junction.getSourceChannel() == sourceChannel);

        final Channel targetChannel0 = junction.getTargetChannel(0);
        Assert.assertTrue(targetChannel0 instanceof DummyReusableChannel);
        Assert.assertTrue(OptimizationUtils.getPredecessorChannel(targetChannel0).getOriginal() == sourceChannel);

        final Channel targetChannel1 = junction.getTargetChannel(1);
        Assert.assertTrue(targetChannel1 instanceof DummyExternalReusableChannel);
        Assert.assertTrue(OptimizationUtils.getPredecessorChannel(targetChannel1) == targetChannel0);
    }

    @Test
    public void updateExistingConversionWithReachedDestination() throws Exception {
        ChannelConversionGraph channelConversionGraph = new ChannelConversionGraph(new Configuration());
        channelConversionGraph.add(reusableToNonReusableChannelConversion);
        channelConversionGraph.add(nonReusableToReusableChannelConversion);
        channelConversionGraph.add(reusableToExternalChannelConversion);
        channelConversionGraph.add(nonReusableToExternalChannelConversion);
        channelConversionGraph.add(externalToNonReusableChannelConversion);

        ExecutionOperator sourceOperator = new DummyExecutionOperator(0, 1, false);
        sourceOperator.getSupportedOutputChannels(0).add(DummyNonReusableChannel.DESCRIPTOR);

        ExecutionOperator destOperator0 = new DummyExecutionOperator(1, 1, false);
        destOperator0.getSupportedInputChannels(0).add(DummyReusableChannel.DESCRIPTOR);

        ExecutionOperator destOperator1 = new DummyExecutionOperator(1, 1, false);
        destOperator1.getSupportedInputChannels(0).add(DummyExternalReusableChannel.DESCRIPTOR);

        final OptimizationContext optimizationContext = new DefaultOptimizationContext(job);
        optimizationContext.addOneTimeOperator(sourceOperator).setOutputCardinality(0, new CardinalityEstimate(1000, 10000, 0.8d));

        final Channel sourceChannel = DummyNonReusableChannel.DESCRIPTOR.createChannel(sourceOperator.getOutput(0), configuration);
        final Channel reusableChannel = nonReusableToReusableChannelConversion.convert(sourceChannel, configuration);
        // We have to pimp the destOperator0 a bit, so that it looks like a regular WayangPlan Operator.
        destOperator0.setContainer(MockFactory.createCompositeOperator().getContainer());
        ExecutionTask destTask0 = new ExecutionTask(destOperator0);
        reusableChannel.addConsumer(destTask0, 0);

        Junction junction = channelConversionGraph.findMinimumCostJunction(
                sourceOperator.getOutput(0),
                Arrays.asList(reusableChannel),
                Arrays.asList(destOperator0.getInput(0), destOperator1.getInput(0)),
                optimizationContext
        );

        Assert.assertTrue(junction.getSourceChannel() == sourceChannel);

        final Channel targetChannel0 = junction.getTargetChannel(0);
        Assert.assertTrue(targetChannel0 == reusableChannel);
        Assert.assertTrue(OptimizationUtils.getPredecessorChannel(targetChannel0) == sourceChannel);

        final Channel targetChannel1 = junction.getTargetChannel(1);
        Assert.assertTrue(targetChannel1 instanceof DummyExternalReusableChannel);
        Assert.assertTrue(OptimizationUtils.getPredecessorChannel(targetChannel1).isCopy());
        Assert.assertTrue(OptimizationUtils.getPredecessorChannel(targetChannel1).getOriginal() == targetChannel0);
    }

    @Test
    public void updateExistingConversionWithTwoOpenChannels() throws Exception {
        ChannelConversionGraph channelConversionGraph = new ChannelConversionGraph(new Configuration());
        channelConversionGraph.add(reusableToNonReusableChannelConversion);
        channelConversionGraph.add(nonReusableToReusableChannelConversion);
        channelConversionGraph.add(reusableToExternalChannelConversion);
        channelConversionGraph.add(nonReusableToExternalChannelConversion);
        channelConversionGraph.add(externalToNonReusableChannelConversion);

        ExecutionOperator sourceOperator = new DummyExecutionOperator(0, 1, false);
        sourceOperator.getSupportedOutputChannels(0).add(DummyNonReusableChannel.DESCRIPTOR);

        ExecutionOperator destOperator0 = new DummyExecutionOperator(1, 1, false);
        destOperator0.getSupportedInputChannels(0).add(DummyReusableChannel.DESCRIPTOR);

        ExecutionOperator destOperator1 = new DummyExecutionOperator(1, 1, false);
        destOperator1.getSupportedInputChannels(0).add(DummyExternalReusableChannel.DESCRIPTOR);

        final OptimizationContext optimizationContext = new DefaultOptimizationContext(job);
        optimizationContext.addOneTimeOperator(sourceOperator).setOutputCardinality(0, new CardinalityEstimate(1000, 10000, 0.8d));

        final Channel sourceChannel = DummyNonReusableChannel.DESCRIPTOR.createChannel(sourceOperator.getOutput(0), configuration);
        final Channel reusableChannel = nonReusableToReusableChannelConversion.convert(sourceChannel, configuration);
        final Channel externalChannel = reusableToExternalChannelConversion.convert(reusableChannel, configuration);

        Junction junction = channelConversionGraph.findMinimumCostJunction(
                sourceOperator.getOutput(0),
                Arrays.asList(reusableChannel, externalChannel),
                Arrays.asList(destOperator0.getInput(0), destOperator1.getInput(0)),
                optimizationContext
        );

        Assert.assertTrue(junction.getSourceChannel() == sourceChannel);

        Assert.assertTrue(sourceChannel.getConsumers().size() == 1);
        ExecutionTask consumer = WayangCollections.getAny(sourceChannel.getConsumers());
        Channel nextChannel = consumer.getOutputChannel(0);
        Assert.assertTrue(nextChannel == reusableChannel);
        Assert.assertTrue(junction.getTargetChannel(0).isCopy() && junction.getTargetChannel(0).getOriginal() == nextChannel);

        consumer = WayangCollections.getSingle(nextChannel.getConsumers());
        nextChannel = consumer.getOutputChannel(0);
        Assert.assertTrue(nextChannel == externalChannel);
        Assert.assertTrue(junction.getTargetChannel(1).isCopy() && junction.getTargetChannel(1).getOriginal() == nextChannel);
    }


}
