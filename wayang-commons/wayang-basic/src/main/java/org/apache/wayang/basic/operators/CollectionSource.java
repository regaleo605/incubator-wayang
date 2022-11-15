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
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.wayang.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.apache.wayang.core.plan.wayangplan.ElementaryOperator;
import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.types.DataSetType;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

/**
 * This source takes as input a Java {@link java.util.Collection}.
 */
public class CollectionSource<T> extends UnarySource<T> implements ElementaryOperator {

    protected final Collection<T> collection;

    protected boolean isHackit;

    public boolean isHackit(){return this.isHackit;}

    public CollectionSource(Collection<T> collection, Class<T> typeClass) {
        this(collection, DataSetType.createDefault(typeClass));
    }

    public CollectionSource(Collection<T> collection, Class<T> typeClass,boolean value) {
        this(collection, DataSetType.createDefault(typeClass),value);
    }

    public CollectionSource(Collection<T> collection, DataSetType<T> type) {
        super(type);
        this.collection = collection;
    }

    public CollectionSource(Collection<T> collection, DataSetType<T> type,boolean value) {
        super(type);
        this.collection = collection;
        this.isHackit = value;
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public CollectionSource(CollectionSource that) {
        super(that);
        this.collection = that.getCollection();
        this.isHackit = that.isHackit();
    }

    public Collection<T> getCollection() {
        return this.collection;
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new DefaultCardinalityEstimator(1d, this.getNumInputs(), this.isSupportingBroadcastInputs(),
                inputCards -> this.collection.size()));
    }

    /**
     * Creates a new instance without any data quanta.
     */
    public static <T> CollectionSource<T> empty(Class<T> typeClass) {
        final CollectionSource<T> instance = new CollectionSource<>(Collections.emptyList(), typeClass);
        instance.setName("{}");
        return instance;
    }

    /**
     * Creates a new instance without any data quanta.
     */
    public static <T> CollectionSource<T> singleton(T value, Class<T> typeClass) {
        final CollectionSource<T> instance = new CollectionSource<>(Collections.singleton(value), typeClass);
        instance.setName("{" + value + "}");
        return instance;
    }
}
