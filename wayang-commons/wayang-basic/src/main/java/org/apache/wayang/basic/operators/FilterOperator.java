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
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.plugin.hackit.core.tags.HackitTag;

import java.util.*;


/**
 * This operator returns a new dataset after filtering by applying predicateDescriptor.
 */
public class FilterOperator<Type> extends UnaryToUnaryOperator<Type, Type> {

    /**
     * Function that this operator applies to the input elements.
     */
    protected final PredicateDescriptor<Type> predicateDescriptor;
    protected TransformationDescriptor<Type,Type> pre;
    protected TransformationDescriptor<Type,Type> post;
    public TransformationDescriptor<Type,Type> getPre(){return this.pre;}
    public TransformationDescriptor<Type,Type> getPost(){return this.post;}

    protected boolean isHackIt = false;
    public boolean isHackIt(){return this.isHackIt;}

    /**
     * Creates a new instance.
     */
    public FilterOperator(PredicateDescriptor.SerializablePredicate<Type> predicateDescriptor, Class<Type> typeClass) {
        this(new PredicateDescriptor<>(predicateDescriptor, typeClass));
    }

    /**
     * Creates a new instance.
     */
    public FilterOperator(PredicateDescriptor<Type> predicateDescriptor) {
        super(DataSetType.createDefault(predicateDescriptor.getInputType()),
                DataSetType.createDefault(predicateDescriptor.getInputType()),
                true);
        this.predicateDescriptor = predicateDescriptor;
    }

    public FilterOperator(PredicateDescriptor<Type> predicateDescriptor
            ,TransformationDescriptor<Type,Type> pre
            ,TransformationDescriptor<Type,Type> post) {
        super(DataSetType.createDefault(predicateDescriptor.getInputType()),
                DataSetType.createDefault(predicateDescriptor.getInputType()),
                true);
        this.predicateDescriptor = predicateDescriptor;
        this.pre = pre;
        this.post = post;
        this.isHackIt = true;
    }

    /**
     * Creates a new instance.
     *
     * @param type type of the dataunit elements
     */
    public FilterOperator(DataSetType<Type> type, PredicateDescriptor.SerializablePredicate<Type> predicateDescriptor) {
        this(new PredicateDescriptor<>(predicateDescriptor, type.getDataUnitType().getTypeClass()), type);
    }

//HackitVersion
    public FilterOperator(DataSetType<Type> type, PredicateDescriptor.SerializablePredicate<Type> predicateDescriptor
            ,TransformationDescriptor<Type,Type> pre
            ,TransformationDescriptor<Type,Type> post) {
        this(new PredicateDescriptor<>(predicateDescriptor, type.getDataUnitType().getTypeClass()), type,pre,post);
    }

    /**
     * Creates a new instance.
     *
     * @param type type of the dataunit elements
     */
    public FilterOperator(PredicateDescriptor<Type> predicateDescriptor, DataSetType<Type> type) {
        super(type, type, true);
        this.predicateDescriptor = predicateDescriptor;
    }

    //Base HackitOperator
    public FilterOperator(PredicateDescriptor<Type> predicateDescriptor, DataSetType<Type> type
            ,TransformationDescriptor<Type,Type> pre
            ,TransformationDescriptor<Type,Type> post) {
        super(type, type, true);
        this.predicateDescriptor = predicateDescriptor;
        this.pre = pre;
        this.post = post;
        this.isHackIt = true;
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public FilterOperator(FilterOperator<Type> that) {
        super(that);
        this.predicateDescriptor = that.getPredicateDescriptor();
        this.isHackIt = that.isHackIt();
        this.pre = that.getPre();
        this.post = that.getPost();
    }

    public PredicateDescriptor<Type> getPredicateDescriptor() {
        return this.predicateDescriptor;
    }

    @Override
    public Optional<org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new FilterOperator.CardinalityEstimator(this.predicateDescriptor, configuration));
    }

    public DataSetType<Type> getType() {
        return this.getInputType();
    }

    /**
     * Custom {@link org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator} for {@link FilterOperator}s.
     */
    private class CardinalityEstimator implements org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator {

        /**
         * The expected selectivity to be applied in this instance.
         */
        private final ProbabilisticDoubleInterval selectivity;

        public CardinalityEstimator(PredicateDescriptor<?> predicateDescriptor, Configuration configuration) {
            this.selectivity = configuration.getUdfSelectivityProvider().provideFor(predicateDescriptor);
        }

        @Override
        public CardinalityEstimate estimate(OptimizationContext optimizationContext, CardinalityEstimate... inputEstimates) {
            Validate.isTrue(inputEstimates.length == FilterOperator.this.getNumInputs());
            final CardinalityEstimate inputEstimate = inputEstimates[0];

            return new CardinalityEstimate(
                    (long) (inputEstimate.getLowerEstimate() * this.selectivity.getLowerEstimate()),
                    (long) (inputEstimate.getUpperEstimate() * this.selectivity.getUpperEstimate()),
                    inputEstimate.getCorrectnessProbability() * this.selectivity.getCorrectnessProbability()
            );
        }
    }
}
