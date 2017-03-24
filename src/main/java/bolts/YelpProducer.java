package bolts;

/**
 * Created by hpnhxxwn on 2017/3/15.
 */
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Properties;
import java.io.IOException;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
//import kafka.consumer.KafkaStream;
import org.apache.commons.lang.ObjectUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import org.mockito.internal.matchers.Null;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.scribe.builder.ServiceBuilder;
import org.scribe.model.OAuthRequest;
import org.scribe.model.Response;
import org.scribe.model.Token;
import org.scribe.model.Verb;
import org.scribe.oauth.OAuthService;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public class YelpProducer extends Thread {

    private static final long serialVersionUID = 1L;
    private static final String API_HOST = "api.yelp.com";
    private static final String DEFAULT_TERM = "dinner";
    private static final String DEFAULT_LOCATION = "San Francisco, CA";
    private static final int SEARCH_LIMIT = 3;
    private static final String SEARCH_PATH = "/v2/search";
    private static final String BUSINESS_PATH = "/v2/business";
    private static final String CONSUMER_KEY = "V6jqDWDZfQQap3ZYfIuf4g";
    private static final String CONSUMER_SECRET = "VaroxPLhxR8bYsw2ziMEOzfg9gk";
    private static final String TOKEN = "VQ5d-6sBKBNv9bcY1pInyE_GJIgoly-x";
    private static final String TOKEN_SECRET = "8i_Sz6jQpWiNM2Q5MGI0OBkEago";
    private static final String APP_ID_= "tBguORzt1OpGKTutlkLNA";
    private static final String APP_SECRET = "RdqHXOvYj40uLr5N5H9gJSN1B9Ah8xEDyKpKxp4vi4sa0apP4phQDsUELnN6iiTq";
    OAuthService service;
    Token accessToken;

    final Properties props = new Properties();
    KafkaProducer kafkaProducer;

    private final static Logger logger = LoggerFactory.getLogger(YelpProducer.class);

    SpoutOutputCollector coll;
    LinkedBlockingQueue<String> statusqueue = new LinkedBlockingQueue<String>();
    //TwitterStream twitterStream;
    private OAuthRequest createOAuthRequest(String path) {
        OAuthRequest request = new OAuthRequest(Verb.GET, "https://" + API_HOST + path);
        return request;
    }
    public String searchByBusinessId(String businessID) {
        OAuthRequest request = createOAuthRequest(BUSINESS_PATH + "/" + businessID);
        return sendRequestAndGetResponse(request);
    }
    public  YelpProducer(String consumerKey, String consumerSecret, String token, String tokenSecret, String kafkaServer) {
        this.service =
                new ServiceBuilder().provider(TwoStepOAuth.class).apiKey(CONSUMER_KEY)
                        .apiSecret(CONSUMER_SECRET).build();
        this.accessToken = new Token(TOKEN, TOKEN_SECRET);

        final Properties properties = new Properties();
        try {
            properties.load(YelpProducer.class.getClassLoader()
                    .getResourceAsStream(utils.Constants.CONFIG_PROPERTIES_FILE));
        } catch (final IOException exception) {
            //LOGGER.error(exception.toString());
            System.exit(1);
        }



        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("producer.type", "async");
        //props.put("partitioner.class", "example.producer.SimplePartitioner");
        props.put("request.required.acks", "1");

        YelpAPICLI yelpApiCli = new YelpAPICLI();
        String[] args = {"bars", "San Francisco, CA"};
        //this.DEFAULT_TERM = args[0];
        //this.DEFAULT_LOCATION = args[1];
        //new JCommander(yelpApiCli, args);


        //YelpSpout yelpApi = new YelpSpout(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, TOKEN_SECRET);
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.setProperty(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG, Integer.toString(5 * 1000));
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProducer = new KafkaProducer(props);
        //queryAPI(yelpApiCli);


    }

    public String searchForBusinessesByLocation(String term, String location) {
        OAuthRequest request = createOAuthRequest(SEARCH_PATH);
        request.addQuerystringParameter("term", term);
        request.addQuerystringParameter("location", location);
        request.addQuerystringParameter("limit", String.valueOf(SEARCH_LIMIT));

        return sendRequestAndGetResponse(request);
    }
    private String sendRequestAndGetResponse(OAuthRequest request) {
        System.out.println("Querying " + request.getCompleteUrl() + " ...");
        this.service.signRequest(this.accessToken, request);
        Response response = request.send();
        System.out.println("Wang " + response.getMessage());
        return response.getBody();
    }
    private void queryAPI(YelpAPICLI yelpApiCli) {
        while (true) {
            String searchResponseJSON = this.searchForBusinessesByLocation(yelpApiCli.term, yelpApiCli.location);

            JSONParser parser = new JSONParser();
            JSONObject response = null;
            try {
                response = (JSONObject) parser.parse(searchResponseJSON);
            } catch (ParseException pe) {
                System.out.println("Error: could not parse JSON response:");
                System.out.println(searchResponseJSON);
                System.exit(1);
            }

            JSONArray businesses = (JSONArray) response.get("businesses");
            if(businesses == null) {
                System.out.println("I'm null");
            } else System.out.println(businesses);

            try {
                System.out.println("miaomiao " + businesses.size());
                for (int i = 0; i < businesses.size(); i++) {
                    JSONObject biz = (JSONObject) businesses.get(i);
                    System.out.println("miaomiaomiao " + biz.toJSONString());

                    System.out.println(String.format(
                            "%s businesses found, querying business info for the current result \"%s\" ...",
                            businesses.size(), biz.get("id").toString()));
                    //String businessResponseJSON = this.searchByBusinessId(biz.get("id").toString());
                    System.out.println(String.format("Result for business \"%s\" found:", biz.get("id").toString()));
                    //System.out.println(businessResponseJSON);


                    ProducerRecord<String, String> data = new ProducerRecord<String, String>("meetup", biz.toJSONString());
                    logger.info("getContextClassLoader(): " + this.getContextClassLoader());
                    logger.info("Begin to send data " + data);
                    kafkaProducer.send(data);
                }

            } catch(java.lang.NullPointerException e) {
                e.printStackTrace();
            }
        }



        /*J
        SONObject firstBusiness = (JSONObject) businesses.get(0);
        String firstBusinessID = firstBusiness.get("id").toString();
        System.out.println(String.format(
                "%s businesses found, querying business info for the top result \"%s\" ...",
                businesses.size(), firstBusinessID));

        // Select the first business and display business details
        String businessResponseJSON = this.searchByBusinessId(firstBusinessID.toString());
        System.out.println(String.format("Result for business \"%s\" found:", firstBusinessID));
        System.out.println(businessResponseJSON);
        */
    }

    /**
     * Command-line interface for the sample Yelp API runner.
     */
    private static class YelpAPICLI {
        @Parameter(names = {"-q", "--term"}, description = "Search Query Term")
        public String term = DEFAULT_TERM;

        @Parameter(names = {"-l", "--location"}, description = "Location to be Queried")
        public String location = DEFAULT_LOCATION;
    }
    public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
        // TODO Auto-generated method stub
        coll=collector;

        //Twitter account authentication from properties file
        final Properties properties = new Properties();
        try {
            properties.load(YelpProducer.class.getClassLoader()
                    .getResourceAsStream(utils.Constants.CONFIG_PROPERTIES_FILE));
        } catch (final IOException exception) {
            //LOGGER.error(exception.toString());
            System.exit(1);
        }


        final Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("producer.type", "async");
        //props.put("partitioner.class", "example.producer.SimplePartitioner");
        props.put("request.required.acks", "1");

        YelpAPICLI yelpApiCli = new YelpAPICLI();
        String[] args = {"bars", "San Francisco, CA"};
        //this.DEFAULT_TERM = args[0];
        //this.DEFAULT_LOCATION = args[1];
        //new JCommander(yelpApiCli, args);


        YelpProducer yelpApi = new YelpProducer(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, TOKEN_SECRET, "localhost:9092");
        queryAPI(yelpApiCli);

    }


    public void nextTuple() {
        // TODO Auto-generated method stub
        String tempst = statusqueue.poll();
        if(tempst==null)
            Utils.sleep(50);
        else
            System.out.println(tempst);
        coll.emit(new Values(tempst));
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub
        declarer.declare(new Fields("tweet"));
    }

    public void run() {
        //YelpProducer producer = new YelpProducer(this.CONSUMER_KEY, this.CONSUMER_SECRET, this.TOKEN, this.TOKEN_SECRET, "localhost:9092");
        YelpAPICLI yelpApiCli = new YelpAPICLI();
        queryAPI(yelpApiCli);
    }
    public final static void main (final String[] args) {
        YelpProducer producer = new YelpProducer(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, TOKEN_SECRET, "localhost:9092");
        producer.run();
    }

}
