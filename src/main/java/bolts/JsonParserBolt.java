package bolts;

/**
 * Created by hpnhxxwn on 2017/3/19.
 */

import java.io.IOException;
import java.util.Map;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import objects.*;

public class JsonParserBolt extends BaseRichBolt {
    private OutputCollector collector;

    public JsonParserBolt() {

    }


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }


    public void execute(Tuple input) {

        ObjectMapper mapper = new ObjectMapper();
        Review review;
        try {
            review = mapper.readValue( input.getString(0), Review.class);
            //System.out.println("HAHAHAHAHAHAHAHAHAHAHAHAHA");
            collector.emit(new Values(review.user_id + "^" + review.text + "^" + review.business_id + "^" + review.stars));
        } catch (JsonParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (JsonMappingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("rawReview"));
    }
}