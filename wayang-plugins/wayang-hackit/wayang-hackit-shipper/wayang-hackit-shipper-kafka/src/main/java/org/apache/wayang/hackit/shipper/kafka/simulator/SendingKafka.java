package org.apache.wayang.hackit.shipper.kafka.simulator;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.wayang.hackit.shipper.kafka.helper.HackitTupleSerializer;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;

import java.util.Properties;

public class SendingKafka {

    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, HackitTupleSerializer.class.getName());
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer(prop);

        ProducerRecord<String,String> producerRecord = new ProducerRecord("signal",new HackitTuple<>("resume"));
        kafkaProducer.send(producerRecord);
        kafkaProducer.close();

    }
}
