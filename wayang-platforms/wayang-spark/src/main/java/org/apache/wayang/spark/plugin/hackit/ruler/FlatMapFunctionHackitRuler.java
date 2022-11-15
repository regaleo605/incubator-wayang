package org.apache.wayang.spark.plugin.hackit.ruler;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.wayang.plugin.hackit.core.iterator.HackitIterator;
import org.apache.wayang.plugin.hackit.core.tags.HackitTag;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;
import org.apache.wayang.plugin.hackit.core.tuple.header.Header;

import java.util.Iterator;
import java.util.Set;

public class FlatMapFunctionHackitRuler<K, I, O>  implements FlatMapFunction<HackitTuple<K, I>, HackitTuple<K, O>> {

    private FlatMapFunction<I, O> function;

    private Function<HackitTuple<Object,I>,HackitTuple<Object,I>> pre;
    private Function<HackitTuple<Object,O>,HackitTuple<Object,O>> post;

    public FlatMapFunctionHackitRuler(FlatMapFunction<I, O> function) {
        this.function = function;
    }

    public FlatMapFunctionHackitRuler(FlatMapFunction<I, O> function
            ,Function<HackitTuple<Object,I>,HackitTuple<Object,I>> pre
            ,Function<HackitTuple<Object,O>,HackitTuple<Object,O>> post) {
        this.function = function;
        this.pre = pre;
        this.post = post;
    }


    @Override
    public Iterator<HackitTuple<K, O>> call(HackitTuple<K, I> kiHackItTuple) throws Exception {
        if(this.pre!=null) {
            kiHackItTuple = (HackitTuple<K, I>) this.pre.call((HackitTuple<Object, I>) kiHackItTuple);
        }
        Iterator<O> result = this.function.call(kiHackItTuple.getValue());
        Header header = kiHackItTuple.getHeader();
        Iterator<HackitTuple<K,O>> iter_result = new HackitIterator<>(
                result,
                record -> {
                    HackitTuple tuple = new HackitTuple<>(header,record);
                    if(this.post!=null) {
                        try {
                             tuple = this.post.call(tuple);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return tuple;
                }
        );
        return iter_result;
    }
}
