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
package org.apache.wayang.plugin.hackit.core.sniffer.shipper;

import org.apache.wayang.plugin.hackit.core.sniffer.shipper.receiver.Receiver;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.sender.Sender;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Shipper is the component that it handle the reception and emission of messages from the main pipeline and sidecar
 * pipeline to enable a smooth connection between them.
 *
 * @param <T_IN> type of the tuple that came from the sidecar to the main pipeline
 * @param <T_OUT> type of the tuple that goes from the main pipeline to sidecar pipeline
 * @param <SenderObj> type of {@link Sender} that the shipper will use
 * @param <ReceiverObj> type of {@link Receiver} that the shipper will use
 */
public abstract class Shipper<T_IN, T_OUT, SenderObj extends Sender<T_OUT>, ReceiverObj extends Receiver<T_IN>> implements Iterator<T_IN>, Serializable {

    /**
     * <code>sender_instance</code> instance that have {@link Sender} implementation
     */
    protected Sender sender_instance;

    /**
     * <code>receiver_instance</code> instance that have {@link Receiver} implementation
     */
    protected Receiver receiver_instance;

    /**
     * Generate an instance of the {@link Sender}, it could be taken from configurations
     *
     * @return {@link Sender} instance
     */
    protected abstract Sender createSenderInstance();

    /**
     * Generate an instance of the {@link Receiver}, it could be taken from configurations
     *
     * @return {@link Receiver} instance
     */
    protected abstract Receiver createReceiverInstance();

    /**
     * Connect with a Message queue service and send a message
     *
     * @param value is the element that it will be sent out from the main pipeline
     */
    public void publish(T_OUT value){
        if(this.sender_instance == null){
            throw new RuntimeException("The Sender of the Shipper is not instantiated");
        }
        this.sender_instance.send(value);
    }

    /**
     * To subscribe as a producer
     */
    public void subscribeAsProducer(){
        this.sender_instance = this.createSenderInstance();
        this.sender_instance.init();
    }

    /**
     * @see #subscribeAsProducer()
     * @param topic list of topic where the messages need to be sent
     */
    public void subscribeAsProducer(String... topic){
        this.subscribeAsProducer("default", topic);
    }

    /**
     * @see #subscribeAsProducer(String...)
     * @param metatopic If the metatopic is different to the Default one, need to be provided here
     */
    public void subscribeAsProducer(String metatopic, String... topic){
        this.subscribeAsProducer();
        ((PSProtocol)this.sender_instance)
                .addExchange(metatopic)
                .addTopic(topic)
        ;
    }

    /**
     * Close connection and send the remaining elements
     */
    public void unsubscribeAsProducer(){
        if( this.sender_instance == null) return;
        this.sender_instance.close();
    }

    /**
     * To subscribe/unsubscribe as a consumer
     * metatopic correspond to EXCHANGE_NAME
     * topics correspond to bindingKeys
     */
    public void subscribeAsConsumer(){
        this.receiver_instance = this.createReceiverInstance();
        this.receiver_instance.init();
    }

    /**
     * @see #subscribeAsConsumer()
     * @param topic list of topic where the consumer will be consuming
     */
    public void subscribeAsConsumer(String... topic){
        this.subscribeAsConsumer("default", topic);
    }

    /**
     * @see #subscribeAsProducer(String...)
     * @param metatopic If the metatopic is different to the Defaults, need to be provided here
     */
    public void subscribeAsConsumer(String metatopic, String... topic){
        this.subscribeAsConsumer();
        ((PSProtocol)this.receiver_instance)
                .addExchange(metatopic)
                .addTopic(topic)
        ;
    }

    /**
     * Close connection and stop consuming elements form the sidecar pipeline
     */
    public void unsubscribeAsConsumer() {
        if( this.receiver_instance == null) return;
        this.receiver_instance.close();
    }

    /**
     * Close the {@link Sender} and {@link Receiver}
     */
    public void close(){
        this.unsubscribeAsConsumer();
        this.unsubscribeAsProducer();
    }

    @Override
    public abstract boolean hasNext();

    @Override
    public abstract T_IN next();

    /**
     * Get the last elements received to be injected on the main pipeline.
     *
     * @return {@link Iterator} with the last element on the {@link org.apache.wayang.plugin.hackit.core.sniffer.shipper.receiver.BufferReceiver}
     */
    public Iterator<T_IN> getNexts(){
        if( this.receiver_instance == null){
            throw new RuntimeException("The Receiver of the Shipper is not instanciated");
        }
        return this.receiver_instance.getElements();
    }

    protected Receiver signal_receiver;

    protected abstract Receiver createSignalReceiver();
    public void subscribeAsSignalReceiver(){
        this.signal_receiver = this.createSignalReceiver();
        this.signal_receiver.init();
    }

    public void unsubscribeAsSignalReceiver() {
        if( this.signal_receiver == null) return;
        this.signal_receiver.close();
    }
    public Iterator<T_IN> getNextSignal(){
        if( this.signal_receiver == null){
            throw new RuntimeException("The Receiver of the Shipper is not instanciated");
        }
        return this.signal_receiver.getElements();
    }
}
