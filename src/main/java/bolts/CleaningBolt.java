package bolts;

/**
 * Created by hpnhxxwn on 2017/3/19.
 */
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CleaningBolt extends BaseRichBolt {
    private static final long serialVersionUID = 2706047697068872387L;
    private static final Logger logger = LoggerFactory.getLogger(CleaningBolt.class);



    private OutputCollector collector;

    public CleaningBolt() {

    }


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("user_id", "text", "business_id", "stars"));
    }


    public void execute(Tuple input) {
        String rawReview = input.getString(0);
        String[] elements = rawReview.split("\\^");
        //do cleaning
        //
        //

        collector.emit(new Values(elements[0], elements[1], elements[2], elements[3]));

    }


}
