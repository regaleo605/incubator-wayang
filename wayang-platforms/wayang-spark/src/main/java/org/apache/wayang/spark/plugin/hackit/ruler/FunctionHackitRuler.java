package org.apache.wayang.spark.plugin.hackit.ruler;

import org.apache.spark.api.java.function.Function;
import org.apache.wayang.plugin.hackit.core.tags.HackitTag;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;

import java.util.Set;

public class FunctionHackitRuler<K,I,O> implements Function<HackitTuple<K,I>,HackitTuple<K,O>> {
    private Function<I, O> function;

    private Function<HackitTuple<Object,I>,HackitTuple<Object,I>> pre;
    private Function<HackitTuple<Object,O>,HackitTuple<Object,O>> post;
    public FunctionHackitRuler(Function<I, O> function) {
        this.function = function;
    }

    public FunctionHackitRuler(Function<I, O> function
            ,Function<HackitTuple<Object,I>,HackitTuple<Object,I>> pre
            ,Function<HackitTuple<Object,O>,HackitTuple<Object,O>> post) {
        this.function = function;
        this.pre = pre;
        this.post = post;
    }

    @Override
    public HackitTuple<K, O> call(HackitTuple<K, I> v1) throws Exception {
        if(pre!=null) v1 = (HackitTuple<K, I>) this.pre.call((HackitTuple<Object, I>) v1);
        O result = this.function.call(v1.getValue());
        HackitTuple tuple = new HackitTuple(v1.getHeader(),result);
        if(this.post!=null) tuple = this.post.call(tuple);
        return tuple;
    }
}