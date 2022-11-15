package org.apache.wayang.hackit.shipper.kafka.helper;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;

import java.util.Map;

public class HackitTupleSerializer<K,T> implements Serializer<HackitTuple<K,T>> {
    @Override
    public void configure(Map configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String arg0, HackitTuple<K,T> arg1) {
        byte[] val = SerializationUtils.serialize(arg1);
        return val;
    }

    @Override
    public void close() {
        Serializer.super.close();
    }

    /*
    Use it in ProducerKafka
    props.put("value.serializer", "com.knoldus.serializers.HackitTupleSerializer");

    try (Producer<String, HackitTuple> producer = new KafkaProducer<>(props)) {
   producer.send(new ProducerRecord<String, HackitTuple>("Debug", tuple));
   System.out.println("Message " + tuple.toString() + " sent !!");
    } catch (Exception e) {
         e.printStackTrace();
    }

          byte[] val = null;
        ObjectMapper mapper = new ObjectMapper();
        try{
            val = mapper.writeValueAsString(arg1).getBytes();
        }catch (Exception e){
            e.printStackTrace();
        }
        return val;

     */
}
