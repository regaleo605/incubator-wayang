package org.apache.wayang.hackit.shipper.kafka.receiver;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.receiver.Receiver;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class SignalReceiverKafka<K,T> extends Receiver<T> {

    private String exchange_name = "default_consumer";

    KafkaConsumer<K,HackitTuple> kafkaConsumer;
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
            this.kafkaConsumer.subscribe(Arrays.asList(this.topic));
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

        ConsumerRecords<K,HackitTuple> records;
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
                        String value = (String) record.value().getValue();
                        System.out.println("Received " + value);
                        if(commands.contains(value)){
                            command.add(record.value());
                            state.set(false);
                        }
                        /*
                        if(value.equalsIgnoreCase("resume")) {
                            command.add(value);
                            state.set(false);

                         */
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
