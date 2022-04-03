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
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.wayang.core.optimizer.cardinality.SwitchForwardCardinalityEstimator;
import org.apache.wayang.core.plan.wayangplan.ElementaryOperator;
import org.apache.wayang.core.plan.wayangplan.InputSlot;
import org.apache.wayang.core.plan.wayangplan.LoopHeadOperator;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.OperatorBase;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.ReflectionUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

/**
 * This operator has three inputs and three outputs.
 */
public class LoopOperator<InputType, ConvergenceType> extends OperatorBase implements ElementaryOperator, LoopHeadOperator {

    public static final int INITIAL_INPUT_INDEX = 0;
    public static final int INITIAL_CONVERGENCE_INPUT_INDEX = 1;
    public static final int ITERATION_INPUT_INDEX = 2;
    public static final int ITERATION_CONVERGENCE_INPUT_INDEX = 3;

    public static final int ITERATION_OUTPUT_INDEX = 0;
    public static final int ITERATION_CONVERGENCE_OUTPUT_INDEX = 1;
    public static final int FINAL_OUTPUT_INDEX = 2;

    /**
     * Function that this operator applies to the input elements.
     */
    protected final PredicateDescriptor<Collection<ConvergenceType>> criterionDescriptor;

    private Integer numExpectedIterations = 0;

    private State state;

    @Override
    public State getState() {
        return state;
    }

    @Override
    public void setState(State state) {
        this.state = state;
    }

    public LoopOperator(Class<InputType> inputTypeClass, Class<ConvergenceType> convergenceTypeClass,
                        PredicateDescriptor.SerializablePredicate<Collection<ConvergenceType>> criterionPredicate,
                        Integer numExpectedIterations) {
        this(DataSetType.createDefault(inputTypeClass),
                DataSetType.createDefault(convergenceTypeClass),
                criterionPredicate,
                numExpectedIterations
        );
    }

    public LoopOperator(DataSetType<InputType> inputType, DataSetType<ConvergenceType> convergenceType,
                        PredicateDescriptor.SerializablePredicate<Collection<ConvergenceType>> criterionPredicate,
                        Integer numExpectedIterations) {
        this(inputType,
                convergenceType,
                new PredicateDescriptor<>(criterionPredicate, ReflectionUtils.specify(Collection.class)),
                numExpectedIterations
        );
    }

    /**
     * Creates a new instance.
     */
    public LoopOperator(DataSetType<InputType> inputType,
                        DataSetType<ConvergenceType> convergenceType,
                        PredicateDescriptor<Collection<ConvergenceType>> criterionDescriptor,
                        Integer numExpectedIterations) {
        super(4, 3, true);
        this.criterionDescriptor = criterionDescriptor;
        this.numExpectedIterations = numExpectedIterations;
        this.state = State.NOT_STARTED;
        this.initializeSlots(inputType, convergenceType);
    }

    /**
     * Creates a copy of the given {@link LoopOperator}.
     *
     * @param that should be copied
     */
    public LoopOperator(LoopOperator<InputType, ConvergenceType> that) {
        super(that);
        this.criterionDescriptor = that.getCriterionDescriptor();
        this.numExpectedIterations = that.getNumExpectedIterations();
        this.state = that.getState();
        this.initializeSlots(that.getInputType(), that.getConvergenceType());
    }

    private void initializeSlots(DataSetType<InputType> inputType, DataSetType<ConvergenceType> convergenceType) {
        this.inputSlots[INITIAL_INPUT_INDEX] = new InputSlot<>("initIn", this, inputType);
        this.inputSlots[INITIAL_CONVERGENCE_INPUT_INDEX] = new InputSlot<>("initConvIn", this, convergenceType);
        this.inputSlots[ITERATION_INPUT_INDEX] = new InputSlot<>("iterIn", this, inputType);
        this.inputSlots[ITERATION_CONVERGENCE_INPUT_INDEX] = new InputSlot<>("convIn", this, convergenceType);

        this.outputSlots[ITERATION_OUTPUT_INDEX] = new OutputSlot<>("iterOut", this, inputType);
        this.outputSlots[ITERATION_CONVERGENCE_OUTPUT_INDEX] = new OutputSlot<>("convOut", this, convergenceType);
        this.outputSlots[FINAL_OUTPUT_INDEX] = new OutputSlot<>("finOut", this, inputType);
    }


    @SuppressWarnings("unchecked")
    public DataSetType<InputType> getInputType() {
        return ((InputSlot<InputType>) this.getInput(INITIAL_INPUT_INDEX)).getType();
    }

    @SuppressWarnings("unchecked")
    public DataSetType<ConvergenceType> getConvergenceType() {
        return ((InputSlot<ConvergenceType>) this.getInput(INITIAL_CONVERGENCE_INPUT_INDEX)).getType();
    }

    public void initialize(Operator initOperator, int initOpOutputIndex, Operator convOperator, int convOpOutputIndex) {
        initOperator.connectTo(initOpOutputIndex, this, INITIAL_INPUT_INDEX);
        convOperator.connectTo(convOpOutputIndex, this, INITIAL_CONVERGENCE_INPUT_INDEX);
    }

    public void initialize(Operator initOperator, Operator convOperator) {
        this.initialize(initOperator, 0, convOperator, 0);
    }

    public void beginIteration(Operator beginOperator, int beginInputIndex, Operator convergeOperator,
                               int convergeInputIndex) {
        this.connectTo(ITERATION_OUTPUT_INDEX, beginOperator, beginInputIndex);
        this.connectTo(ITERATION_CONVERGENCE_OUTPUT_INDEX, convergeOperator, convergeInputIndex);
    }

    public void beginIteration(Operator beginOperator, Operator convergeOperator) {
        this.beginIteration(beginOperator, 0, convergeOperator, 0);
    }

    public void endIteration(Operator endOperator, int endOpOutputIndex, Operator convergeOperator,
                             int convergeOutputIndex) {
        endOperator.connectTo(endOpOutputIndex, this, ITERATION_INPUT_INDEX);
        convergeOperator.connectTo(convergeOutputIndex, this, ITERATION_CONVERGENCE_INPUT_INDEX);
    }

    public void endIteration(Operator endOperator, Operator convergeOperator) {
        this.endIteration(endOperator, 0, convergeOperator, 0);
    }

    public void outputConnectTo(Operator outputOperator, int thatInputIndex) {
        this.connectTo(FINAL_OUTPUT_INDEX, outputOperator, thatInputIndex);
    }

    public void outputConnectTo(Operator outputOperator) {
        this.outputConnectTo(outputOperator, 0);
    }

    public PredicateDescriptor<Collection<ConvergenceType>> getCriterionDescriptor() {
        return this.criterionDescriptor;
    }

    @Override
    public Collection<OutputSlot<?>> getForwards(InputSlot<?> input) {
        assert this.isOwnerOf(input);
        switch (input.getIndex()) {
            case INITIAL_CONVERGENCE_INPUT_INDEX:
            case ITERATION_CONVERGENCE_INPUT_INDEX:
                return Collections.singleton(this.getOutput(ITERATION_CONVERGENCE_OUTPUT_INDEX));
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
            case INITIAL_CONVERGENCE_INPUT_INDEX:
            case ITERATION_CONVERGENCE_INPUT_INDEX:
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
            case ITERATION_CONVERGENCE_OUTPUT_INDEX:
                return Optional.of(new SwitchForwardCardinalityEstimator(
                        INITIAL_CONVERGENCE_INPUT_INDEX,
                        ITERATION_CONVERGENCE_INPUT_INDEX
                ));
            case ITERATION_OUTPUT_INDEX:
            case FINAL_OUTPUT_INDEX:
                return Optional.of(new SwitchForwardCardinalityEstimator(INITIAL_INPUT_INDEX, ITERATION_INPUT_INDEX));
            default:
                throw new IllegalArgumentException("Illegal output index " + outputIndex + ".");
        }
    }

    @Override
    public Collection<OutputSlot<?>> getLoopBodyOutputs() {
        return Arrays.asList(this.getOutput(ITERATION_OUTPUT_INDEX), this.getOutput(ITERATION_CONVERGENCE_OUTPUT_INDEX));
    }

    @Override
    public Collection<OutputSlot<?>> getFinalLoopOutputs() {
        return Collections.singletonList(this.getOutput(FINAL_OUTPUT_INDEX));
    }

    @Override
    public Collection<InputSlot<?>> getLoopBodyInputs() {
        return Arrays.asList(this.getInput(ITERATION_INPUT_INDEX), this.getInput(ITERATION_CONVERGENCE_INPUT_INDEX));
    }

    @Override
    public Collection<InputSlot<?>> getLoopInitializationInputs() {
        return Arrays.asList(this.getInput(INITIAL_INPUT_INDEX), this.getInput(INITIAL_CONVERGENCE_INPUT_INDEX));
    }

    @Override
    public Collection<InputSlot<?>> getConditionInputSlots() {
        return Arrays.asList(this.getInput(INITIAL_CONVERGENCE_INPUT_INDEX), this.getInput(ITERATION_CONVERGENCE_INPUT_INDEX));
    }

    @Override
    public Collection<OutputSlot<?>> getConditionOutputSlots() {
        return Collections.singletonList(this.getOutput(ITERATION_CONVERGENCE_OUTPUT_INDEX));
    }

    public void setNumExpectedIterations(int numExpectedIterations) {
        this.numExpectedIterations = numExpectedIterations;
    }

    @Override
    public int getNumExpectedIterations() {
        return this.numExpectedIterations;
    }

}
