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

package org.apache.wayang.core.optimizer;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.enumeration.PlanEnumerationPruningStrategy;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.wayangplan.InputSlot;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.plan.wayangplan.Slot;
import org.apache.wayang.core.plan.wayangplan.SlotMapping;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Utility methods for the optimization process.
 */
public class OptimizationUtils {

//    private static final Logger logger = LoggerFactory.getLogger(OptimizationUtils.class);

    /**
     * Determine the producing {@link OutputSlot} of this {@link Channel} that lies within a {@link WayangPlan}.
     * We follow non-WayangPlan {@link ExecutionOperator}s because they should merely forward data.
     */
    public static OutputSlot<?> findWayangPlanOutputSlotFor(Channel openChannel) {
        OutputSlot<?> producerOutput = null;
        Channel tracedChannel = openChannel;
        do {
            final ExecutionTask producer = tracedChannel.getProducer();
            final ExecutionOperator producerOperator = producer.getOperator();
            if (!producerOperator.isAuxiliary()) {
                producerOutput = producer.getOutputSlotFor(tracedChannel);
            } else {
                assert producer.getNumInputChannels() == 1;
                tracedChannel = producer.getInputChannel(0);
            }
        } while (producerOutput == null);
        return producerOutput;
    }

    /**
     * Finds the single input {@link Channel} of the given {@code channel}'s producing {@link ExecutionTask}.
     *
     * @param channel whose predecessor is requested
     * @return the preceeding {@link Channel}
     */
    public static Channel getPredecessorChannel(Channel channel) {
        final ExecutionTask producer = channel.getProducer();
        assert producer != null && producer.getNumInputChannels() == 1;
        return producer.getInputChannel(0);
    }

    /**
     * Creates a new {@link PlanEnumerationPruningStrategy} and configures it.
     *
     * @param strategyClass the {@link Class} of the {@link PlanEnumerationPruningStrategy}; must have a default constructor
     * @param configuration provides any potential configuration values
     * @return the configured {@link PlanEnumerationPruningStrategy} instance
     */
    public static <T extends PlanEnumerationPruningStrategy> T createPruningStrategy(Class<T> strategyClass, Configuration configuration) {
        try {
            final T strategy = strategyClass.newInstance();
            strategy.configure(configuration);
            return strategy;
        } catch (InstantiationException | IllegalAccessException e) {
            throw new WayangException(String.format("Could not create pruning strategy for %s.", strategyClass.getCanonicalName()), e);
        }
    }

    /**
     * Collects all {@link Slot}s that are related to the given {@link Slot} either by a {@link SlotMapping} or
     * by {@link OutputSlot}/{@link InputSlot} occupation.
     *
     * @param slot whose related {@link Slot}s are requested
     * @return the related {@link Slot}s including the given {@link Slot}
     */
    public static Set<Slot<?>> collectConnectedSlots(Slot<?> slot) {
        assert slot != null;
        if (slot instanceof InputSlot) {
            return collectConnectedSlots((InputSlot) slot);
        } else {
            assert slot instanceof OutputSlot;
            return collectConnectedSlots((OutputSlot) slot);
        }
    }

    /**
     * Collects all {@link Slot}s that are related to the given {@link InputSlot} either by a {@link SlotMapping} or
     * by {@link OutputSlot}/{@link InputSlot} occupation.
     *
     * @param input whose related {@link Slot}s are requested
     * @return the related {@link Slot}s including the given {@link InputSlot}
     */
    public static Set<Slot<?>> collectConnectedSlots(InputSlot<?> input) {
        Set<Slot<?>> result = new HashSet<>();

        final InputSlot<?> outerInput = input.getOwner().getOutermostInputSlot(input);
        Set<InputSlot<Object>> allInputs = outerInput.getOwner().collectMappedInputSlots(outerInput.unchecked());
        result.addAll(allInputs);

        final OutputSlot<?> outerOutput = outerInput.getOccupant();
        Set<OutputSlot<Object>> allOutputs = outerOutput == null ?
                Collections.emptySet() :
                outerOutput.getOwner().collectMappedOutputSlots(outerOutput.unchecked());
        result.addAll(allOutputs);

        return result;
    }

    /**
     * Collects all {@link Slot}s that are related to the given {@link OutputSlot} either by a {@link SlotMapping} or
     * by {@link OutputSlot}/{@link InputSlot} occupation.
     *
     * @param output whose related {@link Slot}s are requested
     * @return the related {@link Slot}s including the given {@link OutputSlot}
     */
    public static Set<Slot<?>> collectConnectedSlots(OutputSlot<?> output) {
        Set<Slot<?>> result = new HashSet<>();

        final Collection<OutputSlot<Object>> outerOutputs = output.getOwner().getOutermostOutputSlots(output.unchecked());
        for (OutputSlot<Object> outerOutput : outerOutputs) {
            result.addAll(outerOutput.getOwner().collectMappedOutputSlots(outerOutput));
            for (InputSlot<Object> outerInput : outerOutput.getOccupiedSlots()) {
                result.addAll(outerInput.getOwner().collectMappedInputSlots(outerInput.unchecked()));
            }
        }

        return result;
    }

    /**
     * Uses the right part of the logistic regression curve to model a strictly growing function between
     * {@code g0} and 1.
     * @param g0      the starting value at {@code x = 0}
     * @param epsilon the function should have a value {@code >= 1 - epsilon} for {@code x >= x0}
     * @param x0      the convergence {@code x} value
     * @param x       the actual input value to the function
     * @return the function value
     */
    public static double logisticGrowth(double g0, double epsilon, double x0, double x) {
        double a = 2 * (1 - g0);
        double b = Math.pow(a / epsilon - 1, 1 / x0);

        return 1 - a / (1 + Math.pow(b, x));
    }
}
