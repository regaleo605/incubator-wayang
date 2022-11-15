package org.apache.wayang.spark.plugin.hackit.ruler;

import org.apache.spark.api.java.function.Function;
import org.apache.wayang.plugin.hackit.core.tags.HackitTag;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;

import java.util.Set;

public class PredicateHackitRuler<K,I> implements Function<HackitTuple<K,I>,Boolean> {

    private Function<I, Boolean> function;

    private Function<HackitTuple<Object,I>,HackitTuple<Object,I>> pre;
    private Function<HackitTuple<Object,I>,HackitTuple<Object,I>> post;

    public PredicateHackitRuler(Function<I, Boolean> function) {
        this.function = function;
    }

    public PredicateHackitRuler(Function<I, Boolean> function
            ,Function<HackitTuple<Object,I>,HackitTuple<Object,I>> pre
            ,Function<HackitTuple<Object,I>,HackitTuple<Object,I>> post) {
        this.function = function;
        this.pre = pre;
        this.post = post;
    }


    @Override
    public Boolean call(HackitTuple<K, I> v1) throws Exception {
        //System.out.println("SparkFilterHackItOperator is tagging this tuple");
        if(pre!=null) v1 = (HackitTuple<K, I>) this.pre.call((HackitTuple<Object, I>) v1);
        Boolean result = this.function.call(v1.getValue());
        if(this.post!=null && result) v1 = (HackitTuple<K, I>) this.post.call((HackitTuple<Object, I>) v1);
        return result;
    }
}
