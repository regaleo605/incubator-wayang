package org.apache.wayang.hackit.shipper.kafka.simulator;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.wayang.hackit.shipper.kafka.receiver.ReceiverKafka;
import org.apache.wayang.hackit.shipper.kafka.sender.SenderKafka;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.Shipper;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.receiver.Receiver;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.sender.Sender;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Properties;

public class KafkaShipper<T_IN, T_OUT> extends Shipper<T_IN, T_OUT,Sender<T_OUT>
        , Receiver<T_IN>> implements Iterator<T_IN>, Serializable {
    @Override
    public Sender createSenderInstance() {
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        return new SenderKafka(prop);
    }

    @Override
    public Receiver createReceiverInstance() {
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new ReceiverKafka(prop);
    }

    @Override
    public Receiver createSignalReceiver() {
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new ReceiverKafka(prop);
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
