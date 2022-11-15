package org.apache.wayang.hackit.shipper.kafka.ui;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.wayang.hackit.shipper.kafka.helper.HackitTupleDeserializer;
import org.apache.wayang.hackit.shipper.kafka.helper.HackitTupleSerializer;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;

import java.io.IOException;
import java.util.*;

public class ConsoleUI<K,T> {

    private KafkaProducer<String,HackitTuple> signalSender;

    private KafkaProducer<K,HackitTuple<K,T>> tupleSender;
    private KafkaConsumer<K,T> resultReceiver;

    private Scanner scanner = new Scanner(System.in);

    public ConsoleUI(){}

    public void initialize(){
        //Signal Sender
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, HackitTupleSerializer.class.getName());
        signalSender = new KafkaProducer(prop);

        Properties property = new Properties();
        property.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        property.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        property.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, HackitTupleSerializer.class.getName());
        tupleSender = new KafkaProducer(property);

        //Receiver
        Properties prop2 = new Properties();
        final String bootstrapserver = "localhost:9092";
        final String group = "java-group-consumer";
        prop2.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapserver);
        prop2.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop2.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, HackitTupleDeserializer.class.getName());
        prop2.put(ConsumerConfig.GROUP_ID_CONFIG,group);
        prop2.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        resultReceiver = new KafkaConsumer(prop2);
        resultReceiver.subscribe(Arrays.asList("debug"));
    }



    public void close(){
        signalSender.close();
        resultReceiver.close();
    }

    public static void main(String[] args) {
        int giveUp = 6;
        int count = 0;
        ConsoleUI ui = new ConsoleUI();
        ui.initialize();
        System.out.println("Initialize Console UI");
        while(count<10){

            ui.customRun();
            count++;

        }
        System.out.println("Closing Console UI");
        ui.close();
    }

    public void run(){
        this.getNextTuple();
        pressEnterToContinue();
        this.sendSignal();
    }

    public void customRun(){
        this.getNextTuple();
        pressEnterToContinue();
        this.sendCustomSignal();
    }

    public void sendSignal(){
        System.out.print("Enter Command? ");
        String s = scanner.nextLine();
        ProducerRecord<String,HackitTuple> producerRecord = new ProducerRecord("signal",new HackitTuple<>(s));
        signalSender.send(producerRecord);
    }

    public void sendCustomSignal(){
        System.out.print("Enter Sniffer ID?: ");
        String s = scanner.nextLine();
        String topic = "signal"+s;
        System.out.print("Enter Command: ");
        String c = scanner.nextLine();
        ProducerRecord<String,HackitTuple> producerRecord = new ProducerRecord<>(topic,new HackitTuple(c));
        signalSender.send(producerRecord);
    }



    public void getNextTuple(){
        final int giveUp = 50;   int noRecordsCount = 0;
        ConsumerRecords<K, T> consumerRecords;
        while (true) {
            consumerRecords =
                    resultReceiver.poll(1000);

            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            List<HackitTuple<K,T>> result = new ArrayList<>();

            consumerRecords.forEach(record -> {
                HackitTuple<K,T> tuple = (HackitTuple<K,T>) record.value();
                System.out.println(tuple);
                result.add(tuple);

            });
            resultReceiver.commitAsync();
            return ;

        }
        return;
    }


    public static void pressEnterToContinue(){
        try {
            System.in.read();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
