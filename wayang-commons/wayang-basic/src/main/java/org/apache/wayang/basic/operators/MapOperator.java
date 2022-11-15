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
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.types.RecordType;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.wayang.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.plugin.hackit.core.Hackit;
import org.apache.wayang.plugin.hackit.core.tags.HackitTag;
import org.apache.wayang.plugin.hackit.core.tags.LogTag;
import org.apache.wayang.plugin.hackit.core.tags.PauseTag;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;

import java.util.*;

/**
 * A map operator represents semantics as they are known from frameworks, such as Spark and Flink. It pulls each
 * available element from the input slot, applies a function to it, and pushes that element to the output slot.
 */
public class MapOperator<InputType, OutputType> extends UnaryToUnaryOperator<InputType, OutputType> {

    /**
     * Function that this operator applies to the input elements.
     */
    protected final TransformationDescriptor<InputType, OutputType> functionDescriptor;

    protected TransformationDescriptor<InputType,InputType> pre;

    protected TransformationDescriptor<OutputType,OutputType> post;

    protected boolean isHackIt = false;
    public TransformationDescriptor<InputType, InputType> getPre(){return this.pre;}
    public TransformationDescriptor<OutputType,OutputType> getPost(){return this.post;}

    public boolean isHackit(){return this.isHackIt;}

    /**
     * Creates a new instance.
     */
    public MapOperator(FunctionDescriptor.SerializableFunction<InputType, OutputType> function,
                       Class<InputType> inputTypeClass,
                       Class<OutputType> outputTypeClass) {
        this(new TransformationDescriptor<>(function, inputTypeClass, outputTypeClass));
    }


    public MapOperator(FunctionDescriptor.SerializableFunction<InputType, OutputType> function,
                       Class<InputType> inputTypeClass,
                       Class<OutputType> outputTypeClass
            ,FunctionDescriptor.SerializableFunction<InputType,InputType> pre
            ,FunctionDescriptor.SerializableFunction<OutputType,OutputType> post) {
        this(new TransformationDescriptor<>(function, inputTypeClass, outputTypeClass)
                ,new TransformationDescriptor<>(pre,inputTypeClass,inputTypeClass)
                ,new TransformationDescriptor<>(post,outputTypeClass,outputTypeClass));
    }


    /**
     * Creates a new instance.
     */
    public MapOperator(TransformationDescriptor<InputType, OutputType> functionDescriptor) {
        this(functionDescriptor,
                DataSetType.createDefault(functionDescriptor.getInputType()),
                DataSetType.createDefault(functionDescriptor.getOutputType()));
    }


    //Hack it version TransformationDescriptor Set
    public MapOperator(TransformationDescriptor<InputType, OutputType> functionDescriptor
            ,TransformationDescriptor<InputType,InputType> pre
            ,TransformationDescriptor<OutputType,OutputType> post) {
        this(functionDescriptor,
                DataSetType.createDefault(functionDescriptor.getInputType()),
                DataSetType.createDefault(functionDescriptor.getOutputType()),pre,post);
    }


    /**
     * Creates a new instance.
     */
    public MapOperator(TransformationDescriptor<InputType, OutputType> functionDescriptor, DataSetType<InputType> inputType, DataSetType<OutputType> outputType) {
        super(inputType, outputType, true);
        this.functionDescriptor = functionDescriptor;
    }

    //HackIt version super Operator Set
    public MapOperator(TransformationDescriptor<InputType, OutputType> functionDescriptor, DataSetType<InputType> inputType, DataSetType<OutputType> outputType
            ,TransformationDescriptor<InputType,InputType> pre
            ,TransformationDescriptor<OutputType, OutputType> post) {
        super(inputType, outputType, true);
        this.functionDescriptor = functionDescriptor;
        this.isHackIt = true;
        this.pre = pre;
        this.post = post;
    }



    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public MapOperator(MapOperator<InputType, OutputType> that) {
        super(that);
        this.functionDescriptor = that.getFunctionDescriptor();
        this.isHackIt = that.isHackit();
        this.pre = that.getPre();
        this.post = that.getPost();
    }

    /**
     * Creates a new instance that projects the given fields.
     *
     * @param fieldNames the field names for the projected fields
     * @return the new instance
     */
    public static <Input, Output> MapOperator<Input, Output> createProjection(
            Class<Input> inputClass,
            Class<Output> outputClass,
            String... fieldNames) {
        return new MapOperator<>(new ProjectionDescriptor<>(inputClass, outputClass, fieldNames));
    }

    /**
     * Creates a new instance that projects the given fields of {@link Record}s.
     *
     * @param fieldNames the field names for the projected fields
     * @return the new instance
     */
    public static MapOperator<Record, Record> createProjection(
            RecordType inputType,
            String... fieldNames) {
        return new MapOperator<>(ProjectionDescriptor.createForRecords(inputType, fieldNames));
    }

    public TransformationDescriptor<InputType, OutputType> getFunctionDescriptor() {
        return this.functionDescriptor;
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new DefaultCardinalityEstimator(1d, 1, this.isSupportingBroadcastInputs(),
                inputCards -> inputCards[0]));
    }
}
