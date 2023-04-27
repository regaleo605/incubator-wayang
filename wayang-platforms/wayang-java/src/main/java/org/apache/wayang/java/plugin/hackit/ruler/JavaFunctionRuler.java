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

public class JavaFunctionRuler <K,I,O> implements Function<HackitTuple<K,I>,HackitTuple<K,O>> {

    private Function<I,O> function;

    private Function<HackitTuple<Object,I>,HackitTuple<Object,I>> pre;
    private Function<HackitTuple<Object,O>,HackitTuple<Object,O>> post;

    public JavaFunctionRuler(Function<I,O> function){
        this.function = function;
    }

    public JavaFunctionRuler(Function<I,O> function
            ,Function<HackitTuple<Object,I>,HackitTuple<Object,I>> pre
            ,Function<HackitTuple<Object,O>,HackitTuple<Object,O>> post){
        this.function = function;
        this.pre = pre;
        this.post = post;
    }


    @Override
    public HackitTuple<K, O> apply(HackitTuple<K, I> kiHackitTuple) {
        if(this.pre!=null) kiHackitTuple = (HackitTuple<K, I>) pre.apply((HackitTuple<Object, I>) kiHackitTuple);
        O result = this.function.apply(kiHackitTuple.getValue());
        HackitTuple tuple = new HackitTuple(kiHackitTuple.getHeader(),result);
        if(this.post!=null) tuple = post.apply((HackitTuple<Object, O>) kiHackitTuple);
        return tuple;
    }
}
