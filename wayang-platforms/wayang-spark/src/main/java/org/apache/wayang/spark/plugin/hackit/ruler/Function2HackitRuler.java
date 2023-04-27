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

import org.apache.spark.api.java.function.Function2;
import org.apache.wayang.plugin.hackit.core.tags.HackitTag;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;

import java.util.Set;

public class Function2HackitRuler<K,I1,I2,O> implements Function2<HackitTuple<K,I1>,HackitTuple<K,I2>,HackitTuple<K,O>> {

    private Function2<I1,I2,O> function;

    private Set<HackitTag> preTag =null;
    private Set<HackitTag> postTag = null;

    public Function2HackitRuler(Function2<I1,I2,O> func){
        this.function = func;
    }

    public Function2HackitRuler(Function2<I1,I2,O> func, Set<HackitTag> preTag, Set<HackitTag> postTag){
        this.function = func;
        this.preTag = preTag;
        this.postTag = postTag;
    }


    @Override
    public HackitTuple<K, O> call(HackitTuple<K, I1> ki1HackitTuple, HackitTuple<K, I2> ki2HackitTuple) throws Exception {
        //this is 4 output
        if(this.preTag!=null) ki1HackitTuple.addPreTags(preTag);
        O result = this.function.call(ki1HackitTuple.getValue(),ki2HackitTuple.getValue());
        if(this.postTag!=null) ki1HackitTuple.addPostTags(postTag);
        HackitTuple tuple = new HackitTuple(ki1HackitTuple.getHeader(),result);
        return tuple;
    }
}
