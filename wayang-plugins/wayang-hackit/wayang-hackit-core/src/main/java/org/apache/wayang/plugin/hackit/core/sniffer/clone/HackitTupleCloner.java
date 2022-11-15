package org.apache.wayang.plugin.hackit.core.sniffer.clone;

import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;

public class HackitTupleCloner<K,T> implements Cloner<HackitTuple<K,T>, HackitTuple<K,T>> {


    @Override
    public HackitTuple<K,T> clone(HackitTuple<K,T> input) {
        HackitTuple<K,T> tuple = new HackitTuple<>(input.getHeader(),input.getValue());
        return tuple;
    }
}
