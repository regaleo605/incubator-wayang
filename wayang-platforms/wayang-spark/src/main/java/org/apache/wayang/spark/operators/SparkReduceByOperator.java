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

package org.apache.wayang.spark.operators;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.wayang.basic.operators.ReduceByOperator;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.plugin.hackit.core.tags.DebugTag;
import org.apache.wayang.plugin.hackit.core.tags.HackitTag;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;
import org.apache.wayang.spark.channels.BroadcastChannel;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.execution.SparkExecutor;
import org.apache.wayang.spark.plugin.hackit.HackitPairRDD;
import org.apache.wayang.spark.plugin.hackit.HackitRDD;
import scala.Tuple2;

import java.util.*;

/**
 * Spark implementation of the {@link ReduceByOperator}.
 */
public class SparkReduceByOperator<Type, KeyType>
        extends ReduceByOperator<Type, KeyType>
        implements SparkExecutionOperator {

    private TransformationDescriptor<Type,Type> pre;
    private TransformationDescriptor<Type,Type> post;
    private boolean isHackIt = false;

    /**
     * Creates a new instance.
     *
     * @param type             type of the reduce elements (i.e., type of {@link #getInput()} and {@link #getOutput()})
     * @param keyDescriptor    describes how to extract the key from data units
     * @param reduceDescriptor describes the reduction to be performed on the elements
     */
    public SparkReduceByOperator(DataSetType<Type> type, TransformationDescriptor<Type, KeyType> keyDescriptor,
                                 ReduceDescriptor<Type> reduceDescriptor) {
        super(keyDescriptor, reduceDescriptor, type);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public SparkReduceByOperator(ReduceByOperator<Type, KeyType> that) {
        super(that);
        this.isHackIt = that.isHackIt();
        this.pre = that.getPre();
        this.post = that.getPost();
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        if(isHackIt) return hackit(inputs,outputs,sparkExecutor,operatorContext);

        RddChannel.Instance input = (RddChannel.Instance) inputs[0];
        RddChannel.Instance output = (RddChannel.Instance) outputs[0];

        final JavaRDD<Type> inputStream = input.provideRdd();
        final PairFunction<Type, KeyType, Type> keyExtractor =
                sparkExecutor.getCompiler().compileToKeyExtractor(this.keyDescriptor);
        Function2<Type, Type, Type> reduceFunc =
                sparkExecutor.getCompiler().compile(this.reduceDescriptor, this, operatorContext, inputs);
        final JavaPairRDD<KeyType, Type> pairRdd = inputStream.mapToPair(keyExtractor);
        this.name(pairRdd);
        final JavaPairRDD<KeyType, Type> reducedPairRdd =
                pairRdd.reduceByKey(reduceFunc, sparkExecutor.getNumDefaultPartitions());
        this.name(reducedPairRdd);
        final JavaRDD<Type> outputRdd = reducedPairRdd.map(new TupleConverter<>());
        this.name(outputRdd);

        output.accept(outputRdd, sparkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> hackit(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {

        RddChannel.Instance input = (RddChannel.Instance) inputs[0];
        RddChannel.Instance output = (RddChannel.Instance) outputs[0];
        final JavaRDD<HackitTuple<Object, Type>> inputStream = input.provideHackitRDD();

        Function<Type,Type> preFunction = null;
        Function<Type,Type> postFunction = null;
        if(this.pre.getJavaImplementation()!=null) preFunction = sparkExecutor.getCompiler().compile(this.pre,this,operatorContext,inputs);
        if(this.post.getJavaImplementation()!=null) postFunction = sparkExecutor.getCompiler().compile(this.post,this,operatorContext,inputs);


        final PairFunction<Type, KeyType, Type> keyExtractor =
                sparkExecutor.getCompiler().compileToKeyExtractor(this.keyDescriptor);
        Function2<Type, Type, Type> reduceFunc =
                sparkExecutor.getCompiler().compile(this.reduceDescriptor, this, operatorContext, inputs);

        final JavaPairRDD<KeyType, HackitTuple<Object,Type>> pairRdd = new HackitRDD<>(inputStream)
                .mapToPair(keyExtractor,preFunction,null).getPairRDD();
        this.name(pairRdd);
        //reducing
        final JavaPairRDD<KeyType, HackitTuple<Object,Type>> reducedPairRdd = new HackitPairRDD<>(pairRdd).reduceByKey(reduceFunc)
                .getPairRDD();
        this.name(reducedPairRdd);

        final JavaRDD<HackitTuple<Object,Type>> outputRdd = reducedPairRdd.map(new HackitTupleConverter<>(postFunction));
        this.name(outputRdd);

        output.accept(outputRdd, sparkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);



    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkReduceByOperator<>(this.getType(), this.getKeyDescriptor(), this.getReduceDescriptor());
    }

    /**
     * Extracts the value from a {@link scala.Tuple2}.
     * <p><i>TODO: See, if we can somehow dodge all this conversion, which is likely to happen a lot.</i></p>
     */
    private static class TupleConverter<InputType, KeyType>
            implements Function<scala.Tuple2<KeyType, InputType>, InputType> {

        @Override
        public InputType call(scala.Tuple2<KeyType, InputType> scalaTuple) throws Exception {
            return scalaTuple._2;
        }
    }

    private static class HackitTupleConverter<InputType,KeyType>
            implements Function<scala.Tuple2<KeyType,HackitTuple<Object,InputType>>,HackitTuple<Object,InputType>>{

        private Function<InputType,InputType> post;

        public HackitTupleConverter(Function<InputType,InputType> post){
            this.post = post;
        }

        @Override
        public HackitTuple<Object, InputType> call(Tuple2<KeyType, HackitTuple<Object, InputType>> keyTypeHackitTupleTuple2) throws Exception {
            if(this.post!=null){
                return (HackitTuple<Object, InputType>) this.post.call((InputType) keyTypeHackitTupleTuple2._2);
            }else{
                return keyTypeHackitTupleTuple2._2;
            }
        }
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.spark.reduceby.load";
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator =
                SparkExecutionOperator.super.createLoadProfileEstimator(configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.keyDescriptor, configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.reduceDescriptor, configuration);
        return optEstimator;
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        if (index == 0) {
            return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
        } else {
            return Collections.singletonList(BroadcastChannel.DESCRIPTOR);
        }
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        return false;
    }

}
