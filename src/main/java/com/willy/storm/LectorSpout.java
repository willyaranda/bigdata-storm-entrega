package com.willy.storm;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class LectorSpout extends BaseRichSpout {
	private static final long serialVersionUID = 1L;
	public static Logger LOG = Logger.getLogger(LectorSpout.class);
	private FileReader freader;
	private BufferedReader br;
	private String strLine;
	private SpoutOutputCollector collector;
	private String path;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(com.willy.storm.Fields.LINE));
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		try {
			// Open the file that is the first
			// command line parameter
			path = conf.get("filePath").toString();
			freader = new FileReader(path);
			// Get the object of DataInputStream
			br = new BufferedReader(freader);

			// Close the input stream
		} catch (Exception e) {// Catch exception if any
			System.err.println("Error: " + e.getMessage());
		}

		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		try {
			if (!freader.ready()) {
				Thread.sleep(300);
			}
			if ((strLine = br.readLine()) != null) {
				collector.emit(new Values(strLine));
			} else {
				this.close();
			}
		} catch (IOException e) {
			// Silently fail :(
		} catch (InterruptedException e) {
			// Silently fail :(
		}
	}
	
	@Override
	public void close() {
		try {
			freader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
