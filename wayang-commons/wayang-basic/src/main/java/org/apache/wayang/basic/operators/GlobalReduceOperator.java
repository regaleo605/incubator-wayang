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
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.wayang.core.optimizer.cardinality.FixedSizeCardinalityEstimator;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.types.BasicDataUnitType;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.plugin.hackit.core.tags.HackitTag;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * This operator groups the elements of a data set and aggregates the groups.
 */
public class GlobalReduceOperator<Type> extends UnaryToUnaryOperator<Type, Type> {

    protected final ReduceDescriptor<Type> reduceDescriptor;

    protected HackitTag preTag;
    protected HackitTag postTag;

    protected boolean isHackIt = false;

    public HackitTag getPreTag(){return this.preTag;}
    public HackitTag getPostTag(){return this.postTag;}
    public boolean isHackIt(){return this.isHackIt;}

    protected Set<HackitTag> preTags = new HashSet<>();
    protected Set<HackitTag> postTags = new HashSet<>();

    public Set<HackitTag> getPreTags(){return this.preTags;}
    public Set<HackitTag> getPostTags(){return this.postTags;}

    /**
     * Creates a new instance.
     */
    public GlobalReduceOperator(FunctionDescriptor.SerializableBinaryOperator<Type> reduceFunction,
                                Class<Type> typeClass) {
        this(new ReduceDescriptor<>(reduceFunction, typeClass));
    }

    public GlobalReduceOperator(FunctionDescriptor.SerializableBinaryOperator<Type> reduceFunction,
                                Class<Type> typeClass,HackitTag preTag, HackitTag postTag) {
        this(new ReduceDescriptor<>(reduceFunction, typeClass),preTag,postTag);
    }
    public GlobalReduceOperator(FunctionDescriptor.SerializableBinaryOperator<Type> reduceFunction,
                                Class<Type> typeClass,Set<HackitTag> preTag, Set<HackitTag> postTag) {
        this(new ReduceDescriptor<>(reduceFunction, typeClass),preTag,postTag);
    }

    /**
     * Creates a new instance.
     *
     * @param reduceDescriptor describes the reduction to be performed on the elements
     */
    public GlobalReduceOperator(ReduceDescriptor<Type> reduceDescriptor) {
        this(reduceDescriptor, DataSetType.createDefault(
                (BasicDataUnitType<Type>) reduceDescriptor.getInputType().getBaseType()));
    }

    public GlobalReduceOperator(ReduceDescriptor<Type> reduceDescriptor,HackitTag preTag, HackitTag postTag) {
        this(reduceDescriptor, DataSetType.createDefault(
                (BasicDataUnitType<Type>) reduceDescriptor.getInputType().getBaseType()),preTag,postTag);
    }

    public GlobalReduceOperator(ReduceDescriptor<Type> reduceDescriptor,Set<HackitTag> preTag, Set<HackitTag> postTag) {
        this(reduceDescriptor, DataSetType.createDefault(
                (BasicDataUnitType<Type>) reduceDescriptor.getInputType().getBaseType()),preTag,postTag);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public GlobalReduceOperator(GlobalReduceOperator<Type> that) {
        super(that);
        this.reduceDescriptor = that.reduceDescriptor;
        this.preTag = that.getPreTag();
        this.postTag = that.getPostTag();
        this.isHackIt = that.isHackIt();
    }


    /**
     * Creates a new instance.
     *
     * @param reduceDescriptor describes the reduction to be performed on the elements
     * @param type             type of the reduce elements (i.e., type of {@link #getInput()} and {@link #getOutput()})
     */
    public GlobalReduceOperator(ReduceDescriptor<Type> reduceDescriptor, DataSetType<Type> type) {
        super(type, type, true);
        this.reduceDescriptor = reduceDescriptor;
    }

    public GlobalReduceOperator(ReduceDescriptor<Type> reduceDescriptor, DataSetType<Type> type,HackitTag preTag, HackitTag postTag) {
        super(type, type, true);
        this.reduceDescriptor = reduceDescriptor;
        if(preTag != null) this.preTags = Collections.singleton(preTag);
        if(postTag != null) this.postTags = Collections.singleton(postTag);
        this.isHackIt = true;
    }

    public GlobalReduceOperator(ReduceDescriptor<Type> reduceDescriptor, DataSetType<Type> type,Set<HackitTag> preTag, Set<HackitTag> postTag) {
        super(type, type, true);
        this.reduceDescriptor = reduceDescriptor;
        if(preTag!=null) this.preTags = preTag;
        if(postTag!=null) this.postTags = postTag;
        this.isHackIt = true;
    }


    public DataSetType<Type> getType() {
        return this.getInputType();
    }

    public ReduceDescriptor<Type> getReduceDescriptor() {
        return this.reduceDescriptor;
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        // TODO: Come up with a decent way to estimate the "distinctness" of reduction keys.
        return Optional.of(new FixedSizeCardinalityEstimator(1));
    }
}
