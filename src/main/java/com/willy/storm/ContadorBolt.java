package com.willy.storm;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class ContadorBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private String word;
	private HashMap<String, Integer> map = new HashMap<String, Integer>();
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}
	@Override
	public void execute(Tuple tuple) {
		word = tuple.getStringByField(com.willy.storm.Fields.WORD);
		if (map.containsKey(word)) {
			map.put(word, (map.get(word) + 1));
		} else {
			map.put(word, 1);
		}
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(com.willy.storm.Fields.WORD, com.willy.storm.Fields.COUNT));
	}
	@Override
	public void cleanup() {
		for (String word: map.keySet()){
            Integer value = map.get(word);
            System.out.println(word + " -- " +  value);
            collector.emit(new Values(word, value));
		} 
	}
}


