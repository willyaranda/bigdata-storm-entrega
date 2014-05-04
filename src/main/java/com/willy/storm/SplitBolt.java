package com.willy.storm;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.Fields;

public class SplitBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private String line;
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
        this.collector = collector;
	}
	@Override
	public void execute(Tuple tuple) {
		line = tuple.getStringByField(com.willy.storm.Fields.LINE);
		String[] splited = line.split("\\s+");
		for (String word : splited) {
		    collector.emit(new Values(word));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		  declarer.declare(new Fields(com.willy.storm.Fields.WORD));
	}

}
