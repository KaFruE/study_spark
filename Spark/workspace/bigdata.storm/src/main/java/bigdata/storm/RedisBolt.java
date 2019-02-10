package bigdata.storm;

import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;

import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public  class RedisBolt extends AbstractRedisBolt {

	private static final long serialVersionUID = 1L;

	public RedisBolt(JedisPoolConfig config) {
		super(config);
	}

	public RedisBolt(JedisClusterConfig  config) {
		super(config);
	}


	@Override
	public void execute(Tuple input) {

		String date = input.getStringByField("date");
		String log_number = input.getStringByField("log_number");

		JedisCommands jedisCommands = null;

		try {

			jedisCommands = getInstance();
			jedisCommands.sadd(date, log_number);
			
			jedisCommands.expire(date, 604800);

		} catch (JedisConnectionException e) {
			throw new RuntimeException("Exception occurred to JedisConnection"+e, e);
		} catch (JedisException e) {
			System.out.println("Exception occurred from Jedis/Redis" + e);
		} finally {

			if (jedisCommands != null) {
				returnInstance(jedisCommands);
			}
			this.collector.ack(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	protected void process(Tuple arg0) {
		// TODO Auto-generated method stub
		
	}
}