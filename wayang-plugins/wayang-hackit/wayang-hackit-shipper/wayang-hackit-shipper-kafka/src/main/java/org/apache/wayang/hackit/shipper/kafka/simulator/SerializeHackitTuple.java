package org.apache.wayang.hackit.shipper.kafka.simulator;

import org.apache.wayang.plugin.hackit.core.action.Action;
import org.apache.wayang.plugin.hackit.core.action.SendOut;
import org.apache.wayang.plugin.hackit.core.tags.DebugTag;
import org.apache.wayang.plugin.hackit.core.tags.HackitTag;
import org.apache.wayang.plugin.hackit.core.tags.LogTag;
import org.apache.wayang.plugin.hackit.core.tags.PauseTag;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;

public class SerializeHackitTuple {

    public static void main(String[] args) {
        HackitTuple tuple = new HackitTuple(2);
        HackitTag tag = new LogTag();
        HackitTag tag2 = new DebugTag();
        tuple.addPreTag(tag);
        tuple.addPreTag(tag2);
        tuple.addPreTag(null);
        System.out.println(tuple.getPreTag());
        System.out.println(tuple.isSendOut());


    }
}
