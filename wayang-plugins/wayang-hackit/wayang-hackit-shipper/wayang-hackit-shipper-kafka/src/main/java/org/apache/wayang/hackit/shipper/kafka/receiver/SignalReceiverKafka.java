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
package org.apache.wayang.hackit.shipper.kafka.receiver;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.receiver.Receiver;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class SignalReceiverKafka<K,T> extends Receiver<T> {

    private String exchange_name = "default_consumer";

    KafkaConsumer<K,String> kafkaConsumer;
    Properties config;
    List<String> topics;

    String topic;

    public SignalReceiverKafka(Properties config){
        this.config = config;
        this.topics = new ArrayList<>();
    }

    public SignalReceiverKafka(Properties config,String topic){
        this.config = config;
        this.topics = new ArrayList<>();
        this.topic = topic;
    }

    @Override
    public void init() {
        this.kafkaConsumer =
                new KafkaConsumer<>(config);
        if(this.topic!=null){
            this.kafkaConsumer.subscribe(Arrays.asList(this.topic,"signal"));
        } else {
            this.kafkaConsumer.subscribe(Arrays.asList("signal"));
        }
    }
    List<String> commands = Arrays.asList("tuple","operator","skip","resume");

    @Override
    public Iterator<T> getElements() {
        int noMessageFound = 0;
        AtomicBoolean state = new AtomicBoolean(true);

        System.out.println("Waiting For Signal");

        ConsumerRecords<K,String> records;
        while(state.get()){
            records = kafkaConsumer.poll(1000);

            if (records.count() == 0) {
                noMessageFound++;
                if (noMessageFound > 1000)
                    // If no message found count is reached to threshold exit loop.
                    break;
                else
                    continue;
            }
            List<HackitTuple<K,T>> command = new ArrayList<>();
            records.forEach( record -> {
                        String value = (String) record.value();
                        System.out.println("Received " + value);
                        if(commands.contains(value)){
                            command.add((HackitTuple<K, T>) new HackitTuple<>(value));
                            state.set(false);
                        }
                    }
            );

            kafkaConsumer.commitAsync();
            if(command.size()!= 0) return (Iterator<T>) command.iterator();
        }
        return null;
    }

    @Override
    public void close() {
        this.kafkaConsumer.close();
    }

}
