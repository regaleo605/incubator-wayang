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

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.wayang.core.optimizer.cardinality.SwitchForwardCardinalityEstimator;
import org.apache.wayang.core.plan.wayangplan.ElementaryOperator;
import org.apache.wayang.core.plan.wayangplan.InputSlot;
import org.apache.wayang.core.plan.wayangplan.LoopHeadOperator;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.OperatorBase;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.types.DataSetType;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

/**
 * This {@link Operator} repeats a certain subplan of {@link Operator}s for a given number of times.
 */
public class RepeatOperator<Type> extends OperatorBase implements ElementaryOperator, LoopHeadOperator {

    public static final int INITIAL_INPUT_INDEX = 0;
    public static final int ITERATION_INPUT_INDEX = 1;

    public static final int ITERATION_OUTPUT_INDEX = 0;
    public static final int FINAL_OUTPUT_INDEX = 1;

    private final Integer numIterations;

    private State state = State.NOT_STARTED;

    @Override
    public State getState() {
        return state;
    }

    @Override
    public void setState(State state) {
        this.state = state;
    }


    /**
     * Creates a new instance.
     *
     * @param numIterations number of iterations to be performed
     * @param typeClass     of the "circulated" dataset
     */
    public RepeatOperator(Integer numIterations, Class<Type> typeClass) {
        this(numIterations, DataSetType.createDefault(typeClass));
    }

    /**
     * Creates a new instance.
     *
     * @param numIterations number of iterations to be performed
     * @param type          of the "circulated" dataset
     */
    public RepeatOperator(Integer numIterations, DataSetType<Type> type) {
        super(2, 2, false);
        this.initializeSlots(type);
        this.numIterations = numIterations;
    }

    private void initializeSlots(DataSetType<Type> type) {
        this.inputSlots[INITIAL_INPUT_INDEX] = new InputSlot<>("initIn", this, type);
        this.inputSlots[ITERATION_INPUT_INDEX] = new InputSlot<>("iterIn", this, type);
        this.outputSlots[ITERATION_OUTPUT_INDEX] = new OutputSlot<>("iterOut", this, type);
        this.outputSlots[FINAL_OUTPUT_INDEX] = new OutputSlot<>("finOut", this, type);
    }

    /**
     * Creates a copy of the given {@link RepeatOperator}.
     *
     * @param that should be copied
     */
    public RepeatOperator(RepeatOperator<Type> that) {
        super(that);
        this.initializeSlots(that.getType());
        this.numIterations = that.numIterations;
        this.state = that.getState();
    }

    @SuppressWarnings("unchecked")
    public DataSetType<Type> getType() {
        return ((InputSlot<Type>) this.getInput(INITIAL_INPUT_INDEX)).getType();
    }

    public void initialize(Operator initOperator, int initOpOutputIndex) {
        initOperator.connectTo(initOpOutputIndex, this, INITIAL_INPUT_INDEX);
    }

    public void beginIteration(Operator beginOperator, int beginInputIndex) {
        this.connectTo(ITERATION_OUTPUT_INDEX, beginOperator, beginInputIndex);
    }

    public void endIteration(Operator endOperator, int endOpOutputIndex) {
        endOperator.connectTo(endOpOutputIndex, this, ITERATION_INPUT_INDEX);
    }

    public void connectFinalOutputTo(Operator outputOperator, int thatInputIndex) {
        this.connectTo(FINAL_OUTPUT_INDEX, outputOperator, thatInputIndex);
    }

    @Override
    public Collection<OutputSlot<?>> getForwards(InputSlot<?> input) {
        assert this.isOwnerOf(input);
        switch (input.getIndex()) {
            case INITIAL_INPUT_INDEX:
            case ITERATION_INPUT_INDEX:
                return Arrays.asList(this.getOutput(ITERATION_OUTPUT_INDEX), this.getOutput(FINAL_OUTPUT_INDEX));
            default:
                return super.getForwards(input);
        }
    }

    @Override
    public boolean isReading(InputSlot<?> input) {
        assert this.isOwnerOf(input);
        switch (input.getIndex()) {
            case INITIAL_INPUT_INDEX:
            case ITERATION_INPUT_INDEX:
                return true;
            default:
                return super.isReading(input);
        }
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(int outputIndex, Configuration configuration) {
        switch (outputIndex) {
            case ITERATION_OUTPUT_INDEX:
            case FINAL_OUTPUT_INDEX:
                return Optional.of(new SwitchForwardCardinalityEstimator(INITIAL_INPUT_INDEX, ITERATION_INPUT_INDEX));
            default:
                throw new IllegalArgumentException("Illegal output index " + outputIndex + ".");
        }
    }

    @Override
    public Collection<OutputSlot<?>> getLoopBodyOutputs() {
        return Collections.singletonList(this.getOutput(ITERATION_OUTPUT_INDEX));
    }

    @Override
    public Collection<OutputSlot<?>> getFinalLoopOutputs() {
        return Collections.singletonList(this.getOutput(FINAL_OUTPUT_INDEX));
    }

    @Override
    public Collection<InputSlot<?>> getLoopBodyInputs() {
        return Collections.singletonList(this.getInput(ITERATION_INPUT_INDEX));
    }

    @Override
    public Collection<InputSlot<?>> getLoopInitializationInputs() {
        return Collections.singletonList(this.getInput(INITIAL_INPUT_INDEX));
    }

    @Override
    public Collection<InputSlot<?>> getConditionInputSlots() {
        return Collections.emptyList();
    }

    @Override
    public Collection<OutputSlot<?>> getConditionOutputSlots() {
        return Collections.emptyList();
    }

    public int getNumIterations() {
        return this.numIterations;
    }

    @Override
    public int getNumExpectedIterations() {
        return this.getNumIterations();
    }

}
