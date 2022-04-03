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

package org.apache.wayang.graphchi.operators;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.apps.Pagerank;
import edu.cmu.graphchi.datablocks.FloatConverter;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;
import edu.cmu.graphchi.vertexdata.VertexAggregator;
import org.apache.wayang.basic.channels.FileChannel;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.PageRankOperator;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.ConsumerIteratorAdapter;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.core.util.fs.FileSystem;
import org.apache.wayang.core.util.fs.FileSystems;
import org.apache.wayang.core.util.fs.LocalFileSystem;
import org.apache.wayang.graphchi.platform.GraphChiPlatform;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


/**
 * PageRank {@link Operator} implementation for the {@link GraphChiPlatform}.
 */
public class GraphChiPageRankOperator extends PageRankOperator implements GraphChiExecutionOperator {

    private final Logger logger = LogManager.getLogger(this.getClass());

    public GraphChiPageRankOperator(Integer numIterations) {
        super(numIterations);
    }

    public GraphChiPageRankOperator(PageRankOperator pageRankOperator) {
        super(pageRankOperator);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> execute(
            ChannelInstance[] inputChannelInstances,
            ChannelInstance[] outputChannelInstances,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputChannelInstances.length == this.getNumInputs();
        assert outputChannelInstances.length == this.getNumOutputs();

        final FileChannel.Instance inputChannelInstance = (FileChannel.Instance) inputChannelInstances[0];
        final StreamChannel.Instance outputChannelInstance = (StreamChannel.Instance) outputChannelInstances[0];
        try {
            return this.runGraphChi(inputChannelInstance, outputChannelInstance, operatorContext);
        } catch (IOException e) {
            throw new WayangException(String.format("Running %s failed.", this), e);
        }
    }

    private Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> runGraphChi(
            FileChannel.Instance inputFileChannelInstance,
            StreamChannel.Instance outputChannelInstance,
            OptimizationContext.OperatorContext operatorContext)
            throws IOException {

        assert inputFileChannelInstance.wasProduced();

        final String inputPath = inputFileChannelInstance.getSinglePath();
        final String actualInputPath = FileSystems.findActualSingleInputPath(inputPath);
        final FileSystem inputFs = FileSystems.getFileSystem(inputPath).orElseThrow(
                () -> new WayangException(String.format("Could not identify filesystem for \"%s\".", inputPath))
        );

        // Create shards.
        Configuration configuration = operatorContext.getOptimizationContext().getConfiguration();
        String tempDirPath = configuration.getStringProperty("wayang.graphchi.tempdir");
        Random random = new Random();
        String tempFilePath = String.format("%s%s%04x-%04x-%04x-%04x.tmp", tempDirPath, File.separator,
                random.nextInt() & 0xFFFF,
                random.nextInt() & 0xFFFF,
                random.nextInt() & 0xFFFF,
                random.nextInt() & 0xFFFF
        );
        final File tempFile = new File(tempFilePath);
        LocalFileSystem.touch(tempFile);
        tempFile.deleteOnExit();
        String graphName = tempFile.toString();
        // As suggested by GraphChi, we propose to use approximately 1 shard per 1,000,000 edges.
        final int numShards = 2 + (int) inputFs.getFileSize(actualInputPath) / (10 * 1000000);
        if (!new File(ChiFilenames.getFilenameIntervals(graphName, numShards)).exists()) {
            FastSharder sharder = createSharder(graphName, numShards);
            final InputStream inputStream = inputFs.open(actualInputPath);
            sharder.shard(inputStream, FastSharder.GraphInputFormat.EDGELIST);
        } else {
            this.logger.info("Found shards -- no need to preprocess");
        }

        // Run GraphChi.
        GraphChiEngine<Float, Float> engine = new GraphChiEngine<>(graphName, numShards);
        engine.setEdataConverter(new FloatConverter());
        engine.setVertexDataConverter(new FloatConverter());
        engine.setModifiesInedges(false); // Important optimization
        engine.run(new Pagerank(), this.numIterations);

        final ConsumerIteratorAdapter<Tuple2<Long, Float>> consumerIteratorAdapter = new ConsumerIteratorAdapter<>();
        final Consumer<Tuple2<Long, Float>> consumer = consumerIteratorAdapter.getConsumer();
        final Iterator<Tuple2<Long, Float>> iterator = consumerIteratorAdapter.getIterator();

        // Output results.
        VertexIdTranslate trans = engine.getVertexIdTranslate();
        new Thread(
                () -> {
                    try {
                        VertexAggregator.foreach(engine.numVertices(), graphName, new FloatConverter(),
                                (vertexId, vertexValue) -> consumer.accept(new Tuple2<>((long) trans.backward(vertexId), vertexValue)));
                    } catch (IOException e) {
                        throw new WayangException(e);
                    } finally {
                        consumerIteratorAdapter.declareLastAdd();
                    }
                },
                String.format("%s (output)", this)
        ).start();

        Stream<Tuple2<Long, Float>> outputStream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
        outputChannelInstance.accept(outputStream);

        // Model what has been executed.
        final ExecutionLineageNode mainExecutionLineage = new ExecutionLineageNode(operatorContext);
        mainExecutionLineage.add(LoadProfileEstimators.createFromSpecification(
                "wayang.graphchi.pagerank.load.main", configuration
        ));
        mainExecutionLineage.addPredecessor(inputFileChannelInstance.getLineage());

        final ExecutionLineageNode outputExecutionLineage = new ExecutionLineageNode(operatorContext);
        outputExecutionLineage.add(LoadProfileEstimators.createFromSpecification(
                "wayang.graphchi.pagerank.load.output", configuration
        ));
        outputChannelInstance.getLineage().addPredecessor(outputExecutionLineage);

        return mainExecutionLineage.collectAndMark();
    }

    /**
     * Initialize the sharder-program.
     *
     * @param graphName
     * @param numShards
     * @return
     * @throws IOException
     */
    protected static FastSharder createSharder(String graphName, int numShards) throws IOException {
        return new FastSharder<>(
                graphName,
                numShards,
                (vertexId, token) ->
                        (token == null ? 0.0f : Float.parseFloat(token)),
                (from, to, token) ->
                        (token == null ? 0.0f : Float.parseFloat(token)),
                new FloatConverter(),
                new FloatConverter());
    }


    @Override
    public Platform getPlatform() {
        return GraphChiPlatform.getInstance();
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList("wayang.graphchi.pagerank.load.main", "wayang.graphchi.pagerank.load.output");
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(FileChannel.HDFS_TSV_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

}
