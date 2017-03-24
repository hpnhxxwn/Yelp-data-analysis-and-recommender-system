package bolts;

/**
 * Created by hpnhxxwn on 2017/3/16.
 */

import org.apache.kafka.clients.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.List;
import org.apache.kafka.common.serialization.*;
import java.util.Map;
import org.apache.kafka.common.errors.WakeupException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.util.HashMap;
import java.util.concurrent.*;
import java.util.Arrays;
import java.util.ArrayList;
import org.codehaus.jackson.map.ObjectMapper;

import objects.YelpBusinessAPI;

public class YelpComsumer implements Runnable {

    private final KafkaConsumer<String, String> consumer;
    private final List<String> topics;
    private final int id;

    public YelpComsumer(int id,
                        String groupId,
                        List<String> topics) {
        this.id = id;
        this.topics = topics;
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", groupId);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        this.consumer = new KafkaConsumer<String, String>(props);

    }


    public void run() {
        try {
            consumer.subscribe(topics);
            ObjectMapper mapper = new ObjectMapper();
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                    for (ConsumerRecord<String, String> record : records) {
                        Map<String, Object> data = new HashMap<String, Object>();
                        data.put("partition", record.partition());
                        data.put("offset", record.offset());
                        data.put("value", record.value());
                        data.put("key", record.key());
                        data.put("topic", record.topic());
                /*    if (data.get("reviews.excerpt") == null) {
                        continue;
                    }*/
                        System.out.println(this.id + ": " + data.values());
                        YelpBusinessAPI biz = new YelpBusinessAPI();
                        //System.out.println("review object is " + data.get("reviews"));
                        JSONParser parser = new JSONParser();
                        JSONObject value = (JSONObject) parser.parse(record.value());
                        if (record.topic() == "reviews")
                            System.out.println("review except is " + value.get("text"));


                    }
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }

        }
        /*catch (IOException e) {
            e.printStackTrace();
        }*/
        catch (WakeupException e) {
            // ignore for shutdown
        } finally {
            consumer.close();
        }
    }

    public void shutdown() {
        consumer.wakeup();
    }

    public static void main(String[] args) {
        int numConsumers = 3;
        String groupId = "yelp group";
        List<String> topics = Arrays.asList("meetup", "reviews");
        final ExecutorService executor = Executors.newFixedThreadPool(numConsumers);

        final List<YelpComsumer> consumers = new ArrayList<YelpComsumer>();
        for (int i = 0; i < numConsumers; i++) {
            YelpComsumer consumer = new YelpComsumer(i, groupId, topics);
            consumers.add(consumer);
            executor.submit(consumer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (YelpComsumer consumer : consumers) {
                    consumer.shutdown();
                }
                executor.shutdown();
                try {
                    executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

}
