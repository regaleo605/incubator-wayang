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
