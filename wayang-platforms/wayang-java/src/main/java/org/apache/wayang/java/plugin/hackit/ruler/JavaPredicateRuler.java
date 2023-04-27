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

import org.apache.wayang.plugin.hackit.core.Hackit;
import org.apache.wayang.plugin.hackit.core.tags.HackitTag;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;

import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

public class JavaPredicateRuler<K,T> implements Predicate<HackitTuple<K,T>> {

    private Predicate<T> predicate;

    private Function<HackitTuple<Object,T>,HackitTuple<Object,T>> pre;
    private Function<HackitTuple<Object,T>,HackitTuple<Object,T>> post;

    public JavaPredicateRuler(Predicate<T> predicate){
        this.predicate = predicate;
    }

    public JavaPredicateRuler(Predicate<T> predicate
            ,Function<HackitTuple<Object,T>,HackitTuple<Object,T>> pre
            ,Function<HackitTuple<Object,T>,HackitTuple<Object,T>> post){
        this.predicate = predicate;
        this.pre = pre;
        this.post = post;
    }


    @Override
    public boolean test(HackitTuple<K,T> tuple){
        if(this.pre!=null) tuple = (HackitTuple<K, T>) pre.apply((HackitTuple<Object, T>) tuple);
        boolean result = this.predicate.test(tuple.getValue());
        if(this.post!=null && result) tuple = (HackitTuple<K, T>) post.apply((HackitTuple<Object, T>) tuple);
        return result;
    }
}
