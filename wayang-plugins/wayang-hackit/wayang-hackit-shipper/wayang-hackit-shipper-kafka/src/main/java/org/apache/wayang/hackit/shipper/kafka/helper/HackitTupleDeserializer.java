package org.apache.wayang.hackit.shipper.kafka.helper;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;

import java.util.Map;

public class HackitTupleDeserializer<K,T> implements Deserializer<HackitTuple<K,T>> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public HackitTuple<K, T> deserialize(String s, byte[] bytes) {
        HackitTuple<K,T> tuple = (HackitTuple<K, T>) SerializationUtils.deserialize(bytes);
        return tuple;
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }

    /*
       Use it in KafkaConsumer
    props.put("value.deserializer", "com.knoldus.serializers.HackitTupleDeserializer");

  try (KafkaConsumer<String, HackitTuple> tuple = new KafkaConsumer<>(props)) {
    consumer.subscribe(Collections.singletonList(topic));
    while (true) {
        ConsumerRecords<String, HackitTuple> messages = consumer.poll(100);
        for (ConsumerRecord<String, HackitTuple> message : messages) {
          System.out.println("Message received " + message.value().toString());
        }
    }
} catch (Exception e) {
    e.printStackTrace();
}

   ObjectMapper mapper = new ObjectMapper();
        HackitTuple<K,T> tuple = null;
        try{
            tuple = (HackitTuple<K, T>) mapper.readValue(bytes,HackitTuple.class);
        } catch(Exception e){
            e.printStackTrace();
        }
        return tuple;
     */
}
