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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.wayang.hackit.shipper.kafka.receiver.ReceiverKafka;
import org.apache.wayang.hackit.shipper.kafka.receiver.SignalReceiverKafka;
import org.apache.wayang.hackit.shipper.kafka.sender.SenderKafka;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.Shipper;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.receiver.Receiver;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.sender.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Properties;

public class HackitShipperKafka<T_IN, T_OUT> extends Shipper<T_IN, T_OUT, Sender<T_OUT>
        , Receiver<T_IN>> implements Iterator<T_IN>, Serializable {

    private static int signalId = 1;
    private int id = signalId++;
    @Override
    public Sender createSenderInstance() {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new SenderKafka(prop,id);
    }

    @Override
    public Receiver createReceiverInstance() {
        Properties prop = new Properties();
        final String bootstrapserver = "localhost:9092";
        final String group = "java-group-consumerT" + id;
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapserver);
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.GROUP_ID_CONFIG,group);
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
  //      prop.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,100000);
//        prop.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,150000);
   //     prop.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,600000);
     //   prop.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,10);
        return new ReceiverKafka(prop,"debug"+id);
    }

    @Override
    public Receiver createSignalReceiver(){
        Properties prop = new Properties();
        final String bootstrapserver = "localhost:9092";
        final String group = "java-group-consumerS" + id;
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapserver);
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.GROUP_ID_CONFIG,group);
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //return new SignalReceiverKafka(prop);
        return new SignalReceiverKafka(prop,"signal"+id);
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public T_IN next() {
        return null;
    }
}
