/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.wayang.hackit.shipper.kafka;

import org.apache.wayang.plugin.hackit.core.iterator.OneElementIterator;
import org.apache.wayang.plugin.hackit.core.sniffer.clone.Cloner;
import org.apache.wayang.plugin.hackit.core.sniffer.clone.HackitTupleCloner;
import org.apache.wayang.plugin.hackit.core.sniffer.inject.HackitTupleInjector;
import org.apache.wayang.plugin.hackit.core.sniffer.inject.Injector;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.Shipper;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.receiver.Receiver;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.sender.Sender;
import org.apache.wayang.plugin.hackit.core.sniffer.sniff.SingleTagToSniff;
import org.apache.wayang.plugin.hackit.core.sniffer.sniff.Sniff;
import org.apache.wayang.plugin.hackit.core.tags.DebugTag;
import org.apache.wayang.plugin.hackit.core.tags.HackitTag;
import org.apache.wayang.plugin.hackit.core.tags.PauseTag;
import org.apache.wayang.plugin.hackit.core.tags.SkipTag;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;

import java.io.Serializable;
import java.util.*;
import java.util.function.Function;

public class KafkaSniffer<
        K,
        T,
        SentType,
        SenderObj extends Sender<SentType>,
        ReceiverObj extends Receiver<HackitTuple<K,T>>
        >
        implements
        Function<
                HackitTuple<K,T>,
                Iterator<HackitTuple<K,T>>
                >,
        Serializable {

    private transient boolean not_first = false;

    private Injector<HackitTuple<K,T>> hackItInjector;

    private Shipper<HackitTuple<K,T>, SentType, SenderObj, ReceiverObj> shipper;
    private Sniff<HackitTuple<K,T>> hackItSniff;
    private Cloner<HackitTuple<K,T>, SentType> hackItCloner;

    private HackitTupleCloner tupleCloner = new HackitTupleCloner();

    private boolean state = false;

    private List<Set<HackitTag>> preList = new ArrayList<>();
    private List<Set<HackitTag>> postList = new ArrayList<>();

    private HackitTuple<K,T> preSet(HackitTuple<K,T> tuple){
        if(preList.size()!=0) {
            tuple.addTag(preList.get(0));
            preList.remove(0);
            return tuple;
        }

        return tuple;
    }

    private HackitTuple<K,T> postSet(HackitTuple<K,T> tuple){
        if(postList.size()!=0) {
            tuple.addTag(postList.get(0));
            postList.remove(0);
            return tuple;
        }
        return tuple;
    }

    public KafkaSniffer(Shipper<HackitTuple<K,T>, SentType, SenderObj, ReceiverObj> shipper){
        this.hackItInjector = new HackitTupleInjector<>();
        this.shipper = shipper;
        //this.hackItCloner = (Cloner<HackTuple<T>, SentType>) new BasicCloner<HackTuple<T>>();
        //this.hackItCloner = new HackitTupleCloner<>();
        this.hackItSniff= new SingleTagToSniff();
        this.not_first = false;
    }

    @Override
    public Iterator<HackitTuple<K,T>> apply(HackitTuple<K,T> tHackTuple) {
        if(!this.not_first){
            this.shipper.subscribeAsProducer();
            this.shipper.subscribeAsConsumer();
            this.shipper.subscribeAsSignalReceiver();
            this.not_first = true;
        }
        tHackTuple = preSet(tHackTuple);
        //Pause - SendOut - Skip
        if(tHackTuple.getHeader().isHaltJob()){
            state = true;
            if(tHackTuple.getHeader().isSendOut()){
                this.shipper.publish((SentType) String.valueOf(tHackTuple.getValue()));
                tHackTuple.getHeader().clearTags();
                System.out.println("Sent out and pause " + tHackTuple);
                Iterator<HackitTuple<K,T>> signal = this.shipper.getNextSignal();
                String command = (String) signal.next().getValue();
                setAction(command);
                //postSet
                tHackTuple = postSet(tHackTuple);
                Iterator<HackitTuple<K,T>> tuple = new OneElementIterator<>(tHackTuple);
                //Iterator<HackitTuple<K,T>> injection = this.shipper.getNexts();
                return this.hackItInjector.inject(tHackTuple,tuple);

            } else {
                System.out.println("Is being paused " + tHackTuple);
                Iterator<HackitTuple<K,T>> signal = this.shipper.getNextSignal();
                tHackTuple.getHeader().clearPrePostTags();
                //PostSet
                String command = (String) signal.next().getValue();
                setAction(command);
                HackitTuple<K,T> result = postSet(tHackTuple);
                return this.hackItInjector.inject(tHackTuple,new OneElementIterator<>(result));
            }
        }
        //SendOut - Skip
        if(tHackTuple.getHeader().isSendOut()){
            //this.shipper.publish((SentType) this.tupleCloner.clone(tHackTuple));
            this.shipper.publish((SentType) String.valueOf(tHackTuple.getValue()));
            if(tHackTuple.hasCallback()){
                Iterator<HackitTuple<K,T>> injection = this.shipper.getNexts();
                return this.hackItInjector.inject(tHackTuple,injection);
            }
            tHackTuple.getHeader().clearPrePostTags();
            return this.hackItInjector.inject(tHackTuple,new OneElementIterator<>(tHackTuple));
        } else {
            tHackTuple.getHeader().clearPrePostTags();
            return this.hackItInjector.inject(tHackTuple,new OneElementIterator<>(tHackTuple));
        }
    }

    public void setHackItInjector(Injector<HackitTuple<K,T>> hackItInjector) {
        this.hackItInjector = hackItInjector;
    }

    public KafkaSniffer<K, T, SentType, SenderObj, ReceiverObj> setShipper(Shipper<HackitTuple<K,T>,
            SentType, SenderObj, ReceiverObj> shipper) {
        this.shipper = shipper;
        return this;
    }


    public KafkaSniffer<K, T, SentType, SenderObj, ReceiverObj> setHackItSniff(Sniff<HackitTuple<K,T>> hackItSniff) {
        this.hackItSniff = hackItSniff;
        return this;
    }


    public KafkaSniffer<K, T, SentType, SenderObj, ReceiverObj> setHackItCloner(Cloner<HackitTuple<K,T>, SentType> hackItCloner) {
        this.hackItCloner = hackItCloner;
        return this;
    }

    public boolean isInitialized(){
        return this.not_first;
    }

    @Override
    public String toString() {
        return String.format("HackItSniffer{\n first=%s, \n hackItInjector=%s, " +
                        "\n shipper=%s, \n hackItSniff=%s, \n hackItCloner=%s\n}"
                , not_first, hackItInjector, shipper, hackItSniff, hackItCloner);
    }

    public void resume(){
        this.state = false;
    }

    public void setAction(String command){
        switch (command){
            case "tuple": {
                Set<HackitTag> tag = new HashSet<>();
                tag.add(new PauseTag()); tag.add(new DebugTag());
                preList.add(tag);
                break;
            }
            case "operator":{
                Set<HackitTag> tag = new HashSet<>();
                tag.add(new PauseTag()); tag.add(new DebugTag());
                postList.add(tag);
                break;
            }
            case "skip":{
                postList.add(Collections.singleton(new SkipTag()));
                break;
            }
            default : {
                //resuming
            }
        }
    }
}
