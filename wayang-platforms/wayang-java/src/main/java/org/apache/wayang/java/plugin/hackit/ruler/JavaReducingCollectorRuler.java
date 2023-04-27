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
package org.apache.wayang.java.plugin.hackit.ruler;

import org.apache.wayang.plugin.hackit.core.tags.HackitTag;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;
import org.apache.wayang.plugin.hackit.core.tuple.header.Header;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class JavaReducingCollectorRuler<K,T> implements Collector<HackitTuple<K,T>, List<HackitTuple<K,T>>,HackitTuple<K,T>> {

    private final BinaryOperator<T> reduceFunction;

    private Function<HackitTuple<Object,T>,HackitTuple<Object,T>> pre;
    private Function<HackitTuple<Object,T>,HackitTuple<Object,T>> post;

    public JavaReducingCollectorRuler(BinaryOperator<T> reduceFunction){
        this.reduceFunction = reduceFunction;
    }

    public JavaReducingCollectorRuler(BinaryOperator<T> reduceFunction
            ,Function<HackitTuple<Object,T>,HackitTuple<Object,T>> pre
            ,Function<HackitTuple<Object,T>,HackitTuple<Object,T>> post){
        this.reduceFunction = reduceFunction;
        this.pre = pre;
        this.post = post;
    }

    @Override
    public Supplier<List<HackitTuple<K,T>>> supplier() {
        return () -> new ArrayList<>(1);
    }

    @Override
    public BiConsumer<List<HackitTuple<K,T>>, HackitTuple<K,T>> accumulator() {
        return (list, element) -> {
            if (list.isEmpty()) {
                if(this.pre!=null) element = (HackitTuple<K, T>) this.pre.apply((HackitTuple<Object, T>) element);
                list.add(element);
            } else {
                T result = this.reduceFunction.apply(list.get(0).getValue(),element.getValue());
                list.set(0,new HackitTuple<>(element.getHeader(),result));
            }
        };
    }

    @Override
    public BinaryOperator<List<HackitTuple<K,T>>> combiner() {
        return (list1, list2) -> {
            if (list1.isEmpty()) {
                return list2;
            } else if (list2.isEmpty()) {
                return list2;
            } else {
                T result = this.reduceFunction.apply(list1.get(0).getValue(),list2.get(0).getValue());
                //Header merge = list1.get(0).getHeader().mergeHeaderTags(list2.get(0));
                list1.set(0,new HackitTuple<>(result));
                return list1;
            }
        };
    }


    @Override
    public Function<List<HackitTuple<K,T>>, HackitTuple<K,T>> finisher() {
        return list -> {
            assert !list.isEmpty();
            if(this.post!=null){
                HackitTuple tuple = this.post.apply((HackitTuple<Object, T>) list.get(0));
                return tuple;
            } else {
                return list.get(0);
            }
        };
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Collections.emptySet();
    }

}
