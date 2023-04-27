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
