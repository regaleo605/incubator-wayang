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
package org.apache.wayang.spark.plugin.hackit;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.wayang.hackit.shipper.kafka.HackitShipperKafka;
import org.apache.wayang.hackit.shipper.kafka.KafkaSniffer;
import org.apache.wayang.hackit.shipper.kafka.simulator.KafkaShipper;
import org.apache.wayang.plugin.hackit.core.Hackit;
import org.apache.wayang.plugin.hackit.core.tags.HackitTag;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;
import org.apache.wayang.spark.plugin.hackit.ruler.*;

import java.util.*;
import java.util.stream.StreamSupport;

public class HackitRDD<K,T> {

    private JavaRDD<HackitTuple<K, T>> rdd;

    public HackitRDD(JavaRDD<HackitTuple<K, T>> rdd) {
        this.rdd = rdd;
    }


    public JavaRDD getRdd(){return this.rdd;}

    public static <K, T> HackitRDD<K, T> fromJavaRDD(JavaRDD<T> rdd){
        HackitRDD<K, T> ktHackItRDD;
        ktHackItRDD = new HackitRDD<>(
                rdd.map((Function<T, HackitTuple<K, T>>) value -> new HackitTuple<K, T>(value))
        );
        return ktHackItRDD;
    }


    public RDD<T> toRDD(){
        return this.toJavaRDD().rdd();
    }
    public JavaRDD<T> toJavaRDD(){
        return this.rdd.map(hackit -> hackit.getValue());
    }

    public static <K, T> HackitRDD<K, T> wrapDebugRDD(JavaRDD<T> rdd){
        return HackitRDD.fromJavaRDD(rdd);
    }


    public HackitRDD<K, T> filter(Function<T, Boolean> f) {
        HackitRDD<K, T> ktHackItRDD = new HackitRDD<>(
                this.rdd.filter(
                        (Function<HackitTuple<K, T>, Boolean>) new PredicateHackitRuler<K, T>(f)
                )
        );
        return ktHackItRDD;
    }

    public HackitRDD<K, T> filter(Function<T, Boolean> f,Function pre,Function post) {
        HackitRDD<K, T> ktHackItRDD = new HackitRDD<>(
                this.rdd.filter(
                        (Function<HackitTuple<K, T>, Boolean>) new PredicateHackitRuler<K, T>(f,pre,post)
                )
        );
        return ktHackItRDD;
    }


    public <O> HackitRDD<K, O> map(Function f){
        HackitRDD<K, O> ktHackItRDD = new HackitRDD<>(
                this.rdd.map(
                        new FunctionHackitRuler<>(f)
                )
        );
        return ktHackItRDD;
    }

    public <O> HackitRDD<K, O> map(Function f
            ,Function pre, Function post){
        HackitRDD<K, O> ktHackItRDD = new HackitRDD<>(
                this.rdd.map(
                        new FunctionHackitRuler<>(f,pre,post)
                )
        );
        return ktHackItRDD;
    }


    public <O> HackitRDD<K, O> flatMap(FlatMapFunction<T, O> flatMapFunction){
        HackitRDD<K, O> koHackItRDD = new HackitRDD<>(
                this.rdd.flatMap(
                        new FlatMapFunctionHackitRuler<K, T, O>(
                                flatMapFunction
                        )
                )
        );
        return koHackItRDD;
    }

    public <O> HackitRDD<K, O> flatMap(FlatMapFunction<T, O> flatMapFunction
            ,Function pre,Function post){
        HackitRDD<K, O> koHackItRDD = new HackitRDD<>(
                this.rdd.flatMap(
                        new FlatMapFunctionHackitRuler<K, T, O>(
                                flatMapFunction,pre,post
                        )
                )
        );
        return koHackItRDD;
    }

    public <O> HackitRDD<K,O> sniff(){
        //KafkaSniffer sniffer = new KafkaSniffer(new HackitShipperKafka());
        HackitSnifferSpark sniff = new HackitSnifferSpark(new HackitShipperKafka());
        HackitRDD<K,O> hS = new HackitRDD(this.rdd.flatMap(sniff));
        return hS;
    }

    public <KO, O> HackitPairRDD<K, KO, O> mapToPair(PairFunction<T, KO, O> pairFunction){
        HackitPairRDD<K, KO, O> ktDebugRDD = new HackitPairRDD<>(
                this.rdd.mapToPair(
                        new PairFunctionHackitRuler<>(pairFunction)
                )
        );
        return ktDebugRDD;
    }
//For Join
    public <KO, O> HackitPairRDD<K, KO, O> mapToPair(PairFunction<T, KO, O> pairFunction,Set<HackitTag> pre){
        HackitPairRDD<K, KO, O> ktDebugRDD = new HackitPairRDD<>(
                this.rdd.mapToPair(
                        new PrePairFunctionHackitRuler<>(pairFunction,pre)
                )
        );
        return ktDebugRDD;
    }

    public <KO, O> HackitPairRDD<K, KO, O> mapToPair(PairFunction<T, KO, O> pairFunction,Function pre,Function post){
        HackitPairRDD<K, KO, O> ktDebugRDD = new HackitPairRDD<>(
                this.rdd.mapToPair(
                        new PairFunctionHackitRuler<>(pairFunction,pre,post)
                )
        );
        return ktDebugRDD;
    }

    public List<HackitTuple<K,T>> reduce(Function2<T,T,T> func){
        List<HackitTuple<K,T>> result = Collections.singletonList(this.rdd.reduce(new Function2HackitRuler<>(func)));
        return result;
    }

    public List<HackitTuple<K,T>> reduce(Function2<T,T,T> func,Set<HackitTag> preTag,Set<HackitTag> postTag){
        List<HackitTuple<K,T>> result = Collections.singletonList(this.rdd.reduce(new Function2HackitRuler<>(func,preTag,postTag)));
        return result;
    }

       /*
    public HackItRDD<K, T> sniffer(){
        HackItSnifferSpark<Long, String, byte[], SenderMultiChannelRabbitMQ<byte[]>, ReceiverMultiChannelRabbitMQ<Long, String>> lala = new HackItSnifferSpark<Long, String, byte[], SenderMultiChannelRabbitMQ<byte[]>, ReceiverMultiChannelRabbitMQ<Long, String>>(
                new EmptyHackItInjector<>(),
                ele -> true,
                new HackItShipperDirectRabbitMQ(),
                ele -> true, // -> every element will be 'sniffed'
                new DefaultCloner<HackItTuple<Long, String>>()
        );
        return this.sniffer(lala);
    }

     */

    public HackitRDD<K, T> sniffer(HackitSnifferSpark sniffer){
        return new HackitRDD<>(
                this.rdd.flatMap(
                        sniffer
                )
        );
    }


    /*@Override
    public <R> DebugRDD<R> map(Function<T, R> f) {
        return null;
    }

    @Override
    public <K2, V2> DebugRDD<K2, V2> mapToPair(PairFunction<T, K2, V2> f) {
        return null;
    }

    @Override
    public <U> DebugRDD<U> flatMap(FlatMapFunction<T, U> f) {
        return null;
    }

    @Override
    public <K2, V2> DebugRDD<K2, V2> flatMapToPair(PairFlatMapFunction<T, K2, V2> f) {
        return null;
    }
    */

    public void saveAsTextFile(String path) {
        this.toJavaRDD().saveAsTextFile(path);
    }

}
