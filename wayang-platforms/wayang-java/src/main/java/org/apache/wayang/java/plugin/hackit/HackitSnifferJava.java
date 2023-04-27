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
package org.apache.wayang.java.plugin.hackit;

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
import java.util.function.Function;

public class HackitSnifferJava<
        K,
        T,
        Type,
        S extends Sender<Type>,
        R extends Receiver<HackitTuple<K,T>>>
        extends HackitSniffer<K,T,Type,S,R>
        implements Function<HackitTuple<K,T>,Iterator<HackitTuple<K,T>>> {

    public HackitSnifferJava(){
        super();
    }

    public HackitSnifferJava(Injector<HackitTuple<K,T>> injector, Actor<HackitTuple<K,T>> actor,
                              Shipper<HackitTuple<K,T>,Type,S,R> shipper, Sniff<HackitTuple<K,T>> sniff,
                              Cloner<HackitTuple<K,T>,Type> cloner){
        super(injector,actor,shipper,sniff,cloner);
    }

    @Override
    public Iterator<HackitTuple<K, T>> apply(HackitTuple<K, T> ktHackItTuple) {
        Iterator<HackitTuple<K,T>> result = this.apply(ktHackItTuple);
        return result;
    }
}
