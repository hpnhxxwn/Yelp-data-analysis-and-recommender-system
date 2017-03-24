package Topology;

/**
 * Created by hpnhxxwn on 2017/3/19.
 */
import java.io.Serializable;
import java.util.Properties;
import java.util.UUID;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import OAuth.OAuth2Client;
import bolts.*;

public class Topology implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Topology.class);
    static final String TOPOLOGY_NAME = "storm-hbase-count";
    private static final String CONSUMER_KEY = "V6jqDWDZfQQap3ZYfIuf4g";
    private static final String CONSUMER_SECRET = "VaroxPLhxR8bYsw2ziMEOzfg9gk";
    private static final String TOKEN = "VQ5d-6sBKBNv9bcY1pInyE_GJIgoly-x";
    private static final String TOKEN_SECRET = "8i_Sz6jQpWiNM2Q5MGI0OBkEago";

    public static final void main(final String[] args) {
        try {
            String configFileLocation = "config.properties";
            Properties topologyConfig = new Properties();
            topologyConfig.load(ClassLoader.getSystemResourceAsStream(configFileLocation));

            String kafkaserver = topologyConfig.getProperty("kafkaserver");
            String zkConnString = topologyConfig.getProperty("zookeeper");
            System.out.println("zookeeper is " + zkConnString);
            String BusinessTopicName = "meetup";
            String ReviewTopicName = "reviews";
            //start a tweetskafka producer first
            //you need provide the user id you want to follow and kafka server to store the data
            //TweetsKafkaProducer tkProducer = new TweetsKafkaProducer(739682825863995393L,kafkaserver);
            //tkProducer.start();

            //kafka file producer
            OAuth2Client fileProducer = new OAuth2Client("localhost:9092");
            fileProducer.start();


            final Config config = new Config();
            config.setMessageTimeoutSecs(20);
            TopologyBuilder topologyBuilder = new TopologyBuilder();

            System.out.println("Starting Consumers");
            System.out.println("First Consumer is Business Consumer");
            BrokerHosts hosts = new ZkHosts(zkConnString);
            SpoutConfig BusinessSpoutConfig = new SpoutConfig(hosts, BusinessTopicName, "/" + BusinessTopicName, UUID.randomUUID().toString());
            BusinessSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
            KafkaSpout BusinessKafkaSpout = new KafkaSpout(BusinessSpoutConfig);

            System.out.println("Second Consumer is Review Consumer");
            SpoutConfig ReviewSpoutConfig = new SpoutConfig(hosts, ReviewTopicName, "/" + ReviewTopicName, UUID.randomUUID().toString());
            ReviewSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
            KafkaSpout ReviewKafkaSpout = new KafkaSpout(ReviewSpoutConfig);

            // attach the tweet spout to the topology - parallelism of 1
            topologyBuilder.setSpout("ReviewKafkaSpout", ReviewKafkaSpout, 1);
            topologyBuilder.setBolt("ReviewJsonParserBolt", new JsonParserBolt()).shuffleGrouping("ReviewKafkaSpout");
            topologyBuilder.setBolt("ReviewSentenceSplitterBolt", new SentenceSplitterBolt(1)).shuffleGrouping("ReviewJsonParserBolt");
            topologyBuilder.setBolt("ReviewCleaningBolt", new CleaningBolt()).shuffleGrouping("ReviewSentenceSplitterBolt");
            topologyBuilder.setBolt("ReviewHbaseBolt", HBaseUpdateBolt.make(topologyConfig)).shuffleGrouping("ReviewCleaningBolt");

            topologyBuilder.setSpout("BusinessKafkaSpout", BusinessKafkaSpout, 1);
            topologyBuilder.setBolt("BusinessJsonParserBolt", new BusinessJsonParserBolt()).shuffleGrouping("BusinessKafkaSpout");
            topologyBuilder.setBolt("BusinessSentenceSplitterBolt", new SentenceSplitterBolt(1)).shuffleGrouping("BusinessJsonParserBolt");
            topologyBuilder.setBolt("BusinessCleaningBolt", new BusinessCleaningBolt()).shuffleGrouping("BusinessSentenceSplitterBolt");
            topologyBuilder.setBolt("BusinessHbaseBolt", BusinessHBaseUpdateBolt.make(topologyConfig)).shuffleGrouping("BusinessCleaningBolt");

            //Submit it to the cluster or  locally
            if (null != args && 0 < args.length) {
                config.setNumWorkers(3);
                StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
            } else {
                config.setMaxTaskParallelism(10);
                final LocalCluster localCluster = new LocalCluster();
                localCluster.submitTopology(TOPOLOGY_NAME, config, topologyBuilder.createTopology());

                Utils.sleep(360 * 10000);

                LOGGER.info("Shutting down the cluster");
                localCluster.killTopology(TOPOLOGY_NAME);
                localCluster.shutdown();
            }
        } catch (final InvalidTopologyException exception) {
            exception.printStackTrace();
        } catch (final Exception exception) {
            exception.printStackTrace();
        }
    }
}
