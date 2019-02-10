package bigdata.storm;


import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.Maps;
import org.apache.storm.redis.common.config.JedisPoolConfig;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;


public class RealtimeStormTopology {


	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException, IOException, AuthorizationException {  

		StormTopology topology = makeTopology();

		Map<String, String> HBaseConfig = Maps.newHashMap();
		HBaseConfig.put("hbase.rootdir","hdfs://lake1.bigdata:8020/apps/hbase/data");

		Config config = new Config();
		config.setDebug(true);
		config.put("HBASE_CONFIG",HBaseConfig);

		config.put(Config.NIMBUS_HOST, "lake2.bigdata");
		config.put(Config.NIMBUS_THRIFT_PORT, 6627);
		config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
		config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("lake2.bigdata"));

		StormSubmitter.submitTopology(args[0], config, topology);

	}  



	private static StormTopology makeTopology() {

		String zkHost = "lake2.bigdata:2181";
		TopologyBuilder logTopologyBuilder = new TopologyBuilder();
		
		// Spout Bolt
		BrokerHosts brkBost = new ZkHosts(zkHost);
		String topicName = "Log-Topic";
		String zkPathName = "/brokers/topics/Log-Topics";

		SpoutConfig spoutConf = new SpoutConfig(brkBost, topicName, zkPathName, UUID.randomUUID().toString());
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConf);
		
		logTopologyBuilder.setSpout("kafkaSpout", kafkaSpout, 1);
		
		
		// Grouping - SplitBolt & EsperBolt
		logTopologyBuilder.setBolt("splitBolt", new SplitBolt(),1).allGrouping("kafkaSpout");
		logTopologyBuilder.setBolt("esperBolt", new EsperBolt(),1).allGrouping("kafkaSpout");
		
		
		// HBase Bolt
		TupleTableConfig hTableConfig = new TupleTableConfig("LogInfo", "r_key");
		hTableConfig.setZkQuorum("lake1.bigdata");
		hTableConfig.setZkClientPort("2181");
		hTableConfig.setBatch(false);
		hTableConfig.addColumn("cf1", "date");
		hTableConfig.addColumn("cf1", "log_number");
		hTableConfig.addColumn("cf1", "sdata1");
		hTableConfig.addColumn("cf1", "bdata");
		hTableConfig.addColumn("cf1", "sdata2");
		hTableConfig.addColumn("cf1", "ddata");
		hTableConfig.addColumn("cf1", "sdata3");
		hTableConfig.addColumn("cf1", "adata");
		
		HBaseBolt hbaseBolt = new HBaseBolt(hTableConfig);
		logTopologyBuilder.setBolt("HBASE", hbaseBolt, 1).shuffleGrouping("splitBolt");
		
		
		// Redis Bolt
		JedisPoolConfig jedisPoolConfig = new JedisPoolConfig.Builder().setHost("lake1.bigdata").setPort(6379).build();
		RedisBolt redisBolt = new RedisBolt(jedisPoolConfig);
		
		logTopologyBuilder.setBolt("REDIS", redisBolt, 1).shuffleGrouping("esperBolt");

		return logTopologyBuilder.createTopology();
	}

}  
