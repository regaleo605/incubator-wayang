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
package org.apache.wayang.spark.plugin.hackit.ruler;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.wayang.plugin.hackit.core.tags.HackitTag;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;
import scala.Tuple2;

public class PairFunctionHackitRuler <KeyTuple, KeyPair, InputType, OutputType>
        implements PairFunction<HackitTuple<KeyTuple, InputType>, KeyPair, HackitTuple<KeyTuple, OutputType>> {

    private PairFunction<InputType, KeyPair, OutputType> function;

    private Function<HackitTuple<Object,InputType>,HackitTuple<Object,InputType>> pre;
    private Function<HackitTuple<Object,OutputType>,HackitTuple<Object,OutputType>> post;
    public PairFunctionHackitRuler(PairFunction<InputType, KeyPair, OutputType> function) {
        this.function = function;
    }

    public PairFunctionHackitRuler(PairFunction<InputType, KeyPair, OutputType> function
            ,Function<HackitTuple<Object,InputType>,HackitTuple<Object,InputType>> pre
            ,Function<HackitTuple<Object,OutputType>,HackitTuple<Object,OutputType>> post) {
        this.function = function;
        this.pre = pre;
        this.post = post;
    }

    @Override
    public Tuple2<KeyPair, HackitTuple<KeyTuple, OutputType>> call(HackitTuple<KeyTuple, InputType> v1) throws Exception {
        if(this.pre!=null) v1 = (HackitTuple<KeyTuple, InputType>) this.pre.call((HackitTuple<Object, InputType>) v1);
        Tuple2<KeyPair, OutputType> result = this.function.call(v1.getValue());
        HackitTuple<KeyTuple, OutputType> hackItTuple_result = new HackitTuple<>(v1.getHeader(), result._2());
        if(this.post!=null) hackItTuple_result = (HackitTuple<KeyTuple, OutputType>) this.post.call((HackitTuple<Object, OutputType>) hackItTuple_result);
        //this.postTaggingTuple(hackItTuple_result);
        return new Tuple2<>(result._1(), hackItTuple_result);
    }
}
