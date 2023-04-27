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
package org.apache.wayang.java.plugin.hackit;

import org.apache.wayang.java.plugin.hackit.ruler.*;
import org.apache.wayang.plugin.hackit.core.Hackit;
import org.apache.wayang.plugin.hackit.core.tags.HackitTag;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;

import java.util.*;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class HackitStream<K,T> {
    private Stream<HackitTuple<K,T>> stream;

    public HackitStream(Stream<HackitTuple<K,T>> stream){
        this.stream = stream;
    }


    public Stream getStream(){return this.stream;}

    public <O> HackitStream<K, O> map(Function f){
        HackitStream<K, O> hStream = new HackitStream<>(
                this.stream.map(
                        new JavaFunctionRuler<>(f)
                )
        );
        return hStream;
    }

    public <O> HackitStream<K,O> map(Function f
            ,Function pre
            ,Function post){
        HackitStream<K, O> hStream = new HackitStream<>(
                this.stream.map(
                        new JavaFunctionRuler<>(f,pre,post)
                )
        );
        return hStream;
    }

    public <O> HackitStream<K,O> flatMap(Function f
            ,Function pre
            ,Function post){
        System.out.println("I am here");
        HackitStream<K, O> hStream = new HackitStream<>(
                this.stream.flatMap(data ->
                    StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                            new JavaFlatMapRuler<>(f,pre,post).apply(data),Spliterator.ORDERED),false)
                )
        );
        return hStream;
    }

    public Stream<T> toJavaStream(){
        return this.stream.map(x->x.getValue());
    }

    public HackitStream<K,T> filter(Predicate p){
        HackitStream<K,T> hStream = new HackitStream(
                this.stream.filter(new JavaPredicateRuler<>(p))
        );
        return hStream;
    }

    public HackitStream<K,T> filter(Predicate p,Function pre, Function post){
        HackitStream<K,T> hStream = new HackitStream(
                this.stream.filter(new JavaPredicateRuler<>(p,pre,post))
        );
        return hStream;
    }

    public Optional<T> reduce(BinaryOperator bin){
       return this.stream.reduce(new JavaBinaryOperatorRuler<>(bin));
    }

    public Optional<T> reduce(BinaryOperator bin,Set<HackitTag> preTag,Set<HackitTag>postTag){
        return this.stream.reduce(new JavaBinaryOperatorRuler<>(bin,preTag,postTag));
    }

    public Map reduceBy(Function f, BinaryOperator bin){
        Map result = (Map) this.stream.collect(Collectors.groupingBy(new KeyExtractor<>(f),new JavaReducingCollectorRuler<>(bin)));
        return result;
    }

    public Map reduceBy(Function f, BinaryOperator bin,Function pre,Function post){
        Map result = (Map) this.stream.collect(Collectors.groupingBy(
                new KeyExtractor<>(f),new JavaReducingCollectorRuler<>(bin,pre,post)));
        return result;
    }

    public <O> HackitStream<K,O> flatMap(Function f){
        HackitStream<K,O> hS = new HackitStream<>(this.stream.flatMap(new JavaFlatMapRuler<>(f)));
        return hS;
    }


    private static class KeyExtractor<K,I,O> implements Function<HackitTuple<K,I>,O>{
        private Function<I,O> function;
        public KeyExtractor(Function function){
            this.function = function;
        }


        @Override
        public O apply(HackitTuple<K, I> kiHackitTuple) {
            O result = this.function.apply(kiHackitTuple.getValue());
            return result;
        }
    }
}

