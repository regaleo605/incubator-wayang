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