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

import java.util.Set;
import java.util.function.BinaryOperator;

public class JavaBinaryOperatorRuler<K,T> implements BinaryOperator<HackitTuple<K,T>> {

    private BinaryOperator<T> binOp;

    private Set<HackitTag> preTag;
    private Set<HackitTag> postTag;

    public JavaBinaryOperatorRuler(BinaryOperator<T> binOp){
        this.binOp = binOp;

    }

    public JavaBinaryOperatorRuler(BinaryOperator<T> binOp, Set<HackitTag> preTag, Set<HackitTag> postTag){
        this.binOp = binOp;
        this.preTag = preTag;
        this.postTag = postTag;

    }

    @Override
    public HackitTuple<K,T> apply(HackitTuple<K,T> tuple1,HackitTuple<K,T> tuple2){
        T result = this.binOp.apply(tuple1.getValue(),tuple2.getValue());
        if(this.preTag!=null) tuple1.addPreTags(preTag);
        if(this.postTag!=null) tuple1.addPostTags(postTag);
        return new HackitTuple<>(tuple1.getHeader().mergeHeaderTags(tuple2),result);
    }
}
