package org.apache.wayang.plugin.hackit.core.sniffer.inject;

import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

public class HackitTupleInjector<K,T> implements Injector<HackitTuple<K,T>> {

    @Override
    public Iterator<HackitTuple<K,T>> inject(HackitTuple<K,T> element, Iterator<HackitTuple<K,T>> iterator) {
        return
                StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(
                                iterator,
                                Spliterator.ORDERED
                        ),
                        false
                ).filter(
                        (HackitTuple<K,T> _element) -> ! this.is_skip_element(_element)
                ).iterator();
    }

    @Override
    public boolean is_skip_element(HackitTuple<K,T> element) {
        return element.getHeader().isSkip();
    }

    @Override
    public boolean is_halt_job(HackitTuple<K,T> element) {
        return element.getHeader().isHaltJob();
    }
}

