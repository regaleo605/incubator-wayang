package org.apache.wayang.spark.plugin.hackit;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.wayang.hackit.shipper.kafka.KafkaSniffer;
import org.apache.wayang.plugin.hackit.core.sniffer.HackitSniffer;
import org.apache.wayang.plugin.hackit.core.sniffer.actor.Actor;
import org.apache.wayang.plugin.hackit.core.sniffer.clone.Cloner;
import org.apache.wayang.plugin.hackit.core.sniffer.inject.Injector;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.Shipper;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.receiver.Receiver;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.sender.Sender;
import org.apache.wayang.plugin.hackit.core.sniffer.sniff.Sniff;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;

import java.util.Iterator;

public class HackitSnifferSpark<
        K,
        T,
        SentType,
        SenderObj extends Sender<SentType>,
        ReceiverObj extends Receiver<HackitTuple<K,T>>>
    extends KafkaSniffer<K,T,SentType,SenderObj,ReceiverObj>
    implements FlatMapFunction<HackitTuple<K,T>,HackitTuple<K,T>> {

    public HackitSnifferSpark(Shipper<HackitTuple<K,T>, SentType, SenderObj, ReceiverObj> shipper){
        super(shipper);
    }


    @Override
    public Iterator<HackitTuple<K, T>> call(HackitTuple<K, T> ktHackItTuple) throws Exception {
        return this.apply(ktHackItTuple);
    }

}
