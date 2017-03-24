package OAuth;

/**
 * Created by hpnhxxwn on 2017/3/17.
 */
//import org.codehaus.jettison.json.JSONArray;

import java.io.InputStream;
import java.util.Properties;
import java.util.Map;

import bolts.YelpProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OAuth2Client extends Thread {

    private final static Logger logger = LoggerFactory.getLogger(OAuth2Client.class);
    final static Properties props = new Properties();
    private static KafkaProducer kafkaProducer;

    public OAuth2Client(String kafkaServer) {

        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("producer.type", "async");
        //props.put("partitioner.class", "example.producer.SimplePartitioner");
        props.put("request.required.acks", "1");

        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.setProperty(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG, Integer.toString(5 * 1000));
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProducer = new KafkaProducer(props);

    }
    public void queryAPI() throws Exception {
        //Load the properties file
        Properties config = OAuthUtils.getClientConfigProps(OAuthConstants.CONFIG_FILE_PATH);
        //System.out.println("hahahaha" + config
        //        .get(OAuthConstants.ACCESS_TOKEN));
        //Generate the OAuthDetails bean from the config properties file
        OAuth2Details oauthDetails = OAuthUtils.createOAuthDetails(config);
        //Validate Input
        if(!OAuthUtils.isValidInput(oauthDetails)){
            System.out.println("Please provide valid config properties to continue.");
            System.exit(0);
        }

        //Determine operation
        if(oauthDetails.isAccessTokenRequest()){
            //Generate new Access token
            String accessToken = OAuthUtils.getAccessToken(oauthDetails);
            System.out.println(accessToken);
            if(OAuthUtils.isValid(accessToken)){
                System.out.println("Successfully generated Access token for client_credentials grant_type: "+accessToken);
                oauthDetails.setAccessToken(accessToken);
                System.out.println("wangwang" + oauthDetails.getAccessToken());
            }
            else{
                System.out.println("Could not generate Access token for client_credentials grant_type");
            }
        }

        else {
            //Access protected resource from server using OAuth2.0
            // Response from the resource server must be in Json or Urlencoded or xml
            System.out.println("Resource endpoint url: " + oauthDetails.getResourceServerUrl());
            System.out.println("Attempting to retrieve protected resource");
            Map<String, JSONArray> resp = OAuthUtils.getProtectedResource(oauthDetails, "businesses", "");
            //System.out.println(resp);
            JSONArray businesses = resp.get("businesses");
            //JSONParser parser = new JSONParser();
            //JSONArray businesses = (JSONArray) parser.parse(bizs);
            for (int i = 0; i < businesses.size(); i++) {
                JSONObject biz = (JSONObject) businesses.get(i);
                //System.out.println("miaomiaomiao " + biz.toJSONString());

                //System.out.println(String.format(
                //        "%s businesses found, querying business info for the current result \"%s\" ...",
                //        businesses.size(), biz.get("id").toString()));
                //String businessResponseJSON = this.searchByBusinessId(biz.get("id").toString());
                //System.out.println(String.format("Result for business \"%s\" found:", biz.get("id").toString()));

                ProducerRecord<String, String> biz_data = new ProducerRecord<String, String>("meetup", biz.toJSONString().replace("rating", "stars").replace("id", "business_id"));
                logger.info("getContextClassLoader(): " + this.getContextClassLoader());
                logger.info("Begin to send business data " + biz_data);
                kafkaProducer.send(biz_data);
                //System.out.println(businessResponseJSON);
                Map<String, JSONArray> reviews = OAuthUtils.getProtectedResource(oauthDetails, "reviews", biz.get("id").toString());
                JSONArray revs = reviews.get("reviews");
                for (int j = 0; j < revs.size(); j++) {

                    JSONObject review = (JSONObject) revs.get(j);
                    JSONObject user = (JSONObject) review.get("user");
                    review.put("business_id", biz.get("id"));
                    review.put("user_id", user.get("name"));
                    review.remove("user");
                    String rstr = review.toJSONString().replace("rating", "stars").replace("rating", "stars");
                    //System.out.println("Reading review " + j + " for business " + biz.get("id") + ": " + rstr);

                    ProducerRecord<String, String> review_data = new ProducerRecord<String, String>("reviews", rstr);
                    logger.info("getContextClassLoader(): " + this.getContextClassLoader());
                    logger.info("Begin to send review data " + review_data);
                    kafkaProducer.send(review_data);
                }


            }
            //Map<String, String> reviews = OAuthUtils.getProtectedResource(oauthDetails, "reviews")
        }

        //OAuthUtils.getProtectedResource(oauthDetails);
    }
    public void run() {
        //YelpProducer producer = new YelpProducer(this.CONSUMER_KEY, this.CONSUMER_SECRET, this.TOKEN, this.TOKEN_SECRET, "localhost:9092");
        try {
            queryAPI();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
    public final static void main (final String[] args) {
        OAuth2Client producer = new OAuth2Client("localhost:9092");
        producer.run();
    }
}
