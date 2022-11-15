package org.apache.wayang.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.wayang.basic.operators.FlatMapOperator;
import org.apache.wayang.basic.operators.SniffOperator;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.function.FlatMapDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.hackit.shipper.kafka.HackitShipperKafka;
import org.apache.wayang.hackit.shipper.kafka.KafkaSniffer;
import org.apache.wayang.hackit.shipper.kafka.simulator.KafkaShipper;
import org.apache.wayang.hackit.shipper.rabbitmq.HachitShipperDirectRabbitMQ;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;
import org.apache.wayang.spark.channels.BroadcastChannel;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.execution.SparkExecutor;
import org.apache.wayang.spark.plugin.hackit.HackitRDD;
import org.apache.wayang.spark.plugin.hackit.HackitSnifferSpark;

import java.util.*;

public class SparkSniffOperator<InputType,OutputType>
extends SniffOperator<InputType,OutputType>
implements SparkExecutionOperator {

    public SparkSniffOperator(DataSetType inputType, DataSetType outputType,
                                FlatMapDescriptor<InputType, OutputType> functionDescriptor) {
        super(functionDescriptor, inputType, outputType);
    }

    public SparkSniffOperator(SniffOperator<InputType, OutputType> that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final RddChannel.Instance input = (RddChannel.Instance) inputs[0];
        final RddChannel.Instance output = (RddChannel.Instance) outputs[0];
        System.out.println("Its in the sniff");

        //final FlatMapFunction<InputType, OutputType> flatMapFunction =
                //sparkExecutor.getCompiler().compile(this.functionDescriptor, this, operatorContext, inputs);
        KafkaSniffer sniffer = new KafkaSniffer(new HackitShipperKafka());

        final JavaRDD<HackitTuple<Object,InputType>> inputRdd = input.provideHackitRDD();
        final JavaRDD<OutputType> outputRdd = new HackitRDD<>(inputRdd).sniff().getRdd();
        this.name(outputRdd);
        output.accept(outputRdd, sparkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkFlatMapOperator<>(this.getInputType(), this.getOutputType(), this.getFunctionDescriptor());
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.spark.flatmap.load";
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator =
                SparkExecutionOperator.super.createLoadProfileEstimator(configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.functionDescriptor, configuration);
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
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        return false;
    }
}
