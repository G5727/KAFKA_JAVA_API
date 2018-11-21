package main.java.KafkaAPI;


import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.ExecutionException;

public class RunKafka {
    public static void main(String args[])
    {
        //runProducer();
        runConsumer();
    }

    static void runProducer()
    {
        Producer<Long,String> producer = FirstProducer.createProducer();

        for(int index=0;index<KafkaConstants.MESSAGE_COUNT;index++)
        {
            final ProducerRecord<Long,String> record = new ProducerRecord<Long, String>(KafkaConstants.TOPIC_NAME,"This is record from producer "+index);

            try {
                RecordMetadata metadata = producer.send(record).get();
            } catch (InterruptedException e) {
                System.out.println("Error in sending record");
                System.out.println(e);
            } catch (ExecutionException e) {
                System.out.println("Error in sending record");
                System.out.println(e);
            }

        }


    }
    static void runConsumer()
    {

        Consumer<Long,String> consumer = FirstConsumer.createConsumer();

        int noofmsg = 0;

        while(true)
        {
            final ConsumerRecords<Long,String> consumerRecords = consumer.poll(1000);
            if(consumerRecords.count() ==0)
            {
                noofmsg++;

                if(noofmsg>KafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
                    break;
                else
                    continue;
            }

            consumerRecords.forEach(record -> {
                System.out.println("Record value " + record.value());
                //System.out.println("Record partition " + record.partition());
                //System.out.println("Record offset " + record.offset());
            });
            consumer.commitAsync();
        }
        consumer.close();

    }
}
