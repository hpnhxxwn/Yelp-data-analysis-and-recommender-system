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

public class BusinessCleaningBolt extends BaseRichBolt {
    private static final long serialVersionUID = 2706047697068872387L;
    private static final Logger logger = LoggerFactory.getLogger(CleaningBolt.class);



    private OutputCollector collector;

    public BusinessCleaningBolt() {

    }


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("business_id", "name", "star", "city"));
    }


    public void execute(Tuple input) {
        String rawBusiness = input.getString(0);
        String[] elements = rawBusiness.split("\\^");
        //do cleaning
        //
        //

        collector.emit(new Values(elements[1], elements[0], elements[2], elements[3]));

    }


}
