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

import org.apache.wayang.plugin.hackit.core.iterator.HackitIterator;
import org.apache.wayang.plugin.hackit.core.tags.HackitTag;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;
import org.apache.wayang.plugin.hackit.core.tuple.header.Header;

import java.util.Iterator;
import java.util.Set;
import java.util.function.Function;

public class JavaFlatMapRuler<K,I,O> implements Function<HackitTuple<K,I>, Iterator<HackitTuple<K,O>>> {

    private Function<I,Iterable<O>> function;

    private Function<HackitTuple<Object,I>,HackitTuple<Object,I>> pre;
    private Function<HackitTuple<Object,O>,HackitTuple<Object,O>> post;

    public JavaFlatMapRuler(Function<I,Iterable<O>> function){
        this.function = function;
    }

    public JavaFlatMapRuler(Function<I,Iterable<O>> function
            ,Function<HackitTuple<Object,I>,HackitTuple<Object,I>> pre
            ,Function<HackitTuple<Object,O>,HackitTuple<Object,O>> post){
        this.function = function;
        this.pre = pre;
        this.post = post;
    }
    @Override
    public Iterator<HackitTuple<K,O>> apply(HackitTuple<K,I> value){
        if(this.pre!=null) value = (HackitTuple<K, I>) this.pre.apply((HackitTuple<Object, I>) value);
        Iterator<O> result = this.function.apply(value.getValue()).iterator();
        //if(this.postTag!=null)value.addPostTags(postTag);
        Header header = value.getHeader();
        Iterator<HackitTuple<K,O>> iter_result = new HackitIterator<>(
                result,
                record ->{
                    HackitTuple tuple = new HackitTuple(header,record);
                    if(post!=null) tuple = this.post.apply(tuple);
                    return tuple;
                }
        );
        return iter_result;
    }
}
