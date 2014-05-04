package com.willy.storm;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.utils.Utils;
import backtype.storm.tuple.Fields;


public class ContadorPalabrasTopologia {
	private TopologyBuilder builder = new TopologyBuilder();
	private static Config conf = new Config();
	private LocalCluster cluster;

	public ContadorPalabrasTopologia() {
	
		builder.setSpout("lectorSpout", new LectorSpout(), 1);
		
		builder.setBolt("splitBolt", new SplitBolt(), 10)
				.shuffleGrouping("lectorSpout");
		
		// Al usar .fieldsGrouping, todo lo igual ir‡ al mismo ContadorBolt,
		// por lo que pasamos de distribuir de una forma uniforme pero aleatoria
		// los datos a que el sistema nos distribuya un mismo texto a un mismo bolt
		builder.setBolt("contadorBolt", new ContadorBolt(), 10)
			.fieldsGrouping("splitBolt", new Fields(com.willy.storm.Fields.WORD));
	}
	public TopologyBuilder getBuilder() {
		return builder;
	}
	public LocalCluster getLocalCluster() {
		return cluster;
	}
	public Config getConf() {
		return conf;
	}
	public void runLocal(int runTime, String filePath) {
		conf.setDebug(false);
		conf.put("filePath", filePath);
		cluster = new LocalCluster();
		cluster.submitTopology("ContadorPalabrasTopologia", conf, builder.createTopology());
		if (runTime > 0) {
			Utils.sleep(runTime);
			shutDownLocal();
		}
	}
	public void shutDownLocal() {
		if (cluster != null) {
			cluster.killTopology("ContadorPalabrasTopologia");
			cluster.shutdown();
		}
	}
	public void runCluster(String name, String redisHost)
			throws AlreadyAliveException, InvalidTopologyException {
		conf.setNumWorkers(20);
		StormSubmitter.submitTopology(name, conf, builder.createTopology());
	}
		
	
	public static void main(String[] args) throws Exception {
		
		ContadorPalabrasTopologia topology = new ContadorPalabrasTopologia();
		
		if (args != null && args.length < 1) {
			System.out.println(args[0].toString());
			System.out.println("Please, provide the path to the text file to be wordcounted");
		} else if (args != null && args.length == 1) {
			topology.runLocal(10000, args[0]);
		} else if (args != null && args.length == 2) {
			topology.runCluster(args[0], args[1]);
		}
	}
}
