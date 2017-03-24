package bolts;

/**
 * Created by hpnhxxwn on 2017/3/19.
 */
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SentenceSplitterBolt extends BaseRichBolt{
    private static final long serialVersionUID = 5151173513759399636L;

    private int minWordLength=1;

    private OutputCollector collector;

    public SentenceSplitterBolt(int minWordLength) {
        this.minWordLength = minWordLength;
    }


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }


    public void execute(Tuple input) {

        String review = input.getString(0);
        String text = review.replaceAll("\\r|\\n", "").toLowerCase();
        String[] elements = text.split("\\n");
        for (String element : elements) {
            if (element.length() >= minWordLength) {
                collector.emit(new Values(element));
                System.out.println(element);
            }
        }
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("rawReview"));
    }
}
