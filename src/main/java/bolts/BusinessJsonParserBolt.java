package bolts;

/**
 * Created by hpnhxxwn on 2017/3/19.
 */
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


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

public class BusinessJsonParserBolt extends BaseRichBolt {
    private OutputCollector collector;

    public BusinessJsonParserBolt() {

    }


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }


    public void execute(Tuple input) {

        ObjectMapper mapper = new ObjectMapper();
        Business business;
        try {
            business = mapper.readValue( input.getString(0), Business.class);
            //System.out.println("MIAOMIAOMIAOMIAOMIAOMIAOMIAOMIAO");
            collector.emit(new Values(business.name+"^"+business.business_id+"^"+business.stars+"^"+business.city));
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
