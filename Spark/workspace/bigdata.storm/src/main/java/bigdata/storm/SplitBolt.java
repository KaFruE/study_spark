package bigdata.storm;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


public class SplitBolt extends BaseBasicBolt{

	private static final long serialVersionUID = 1L;

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {

		String tValue = tuple.getString(0);  
		
		String[] receiveData = tValue.split(",");
				
		collector.emit(new Values(	new StringBuffer(receiveData[0]).reverse() + "-" + receiveData[1]  , 
									receiveData[0], receiveData[1], receiveData[2], receiveData[3],
									receiveData[4], receiveData[5], receiveData[6], receiveData[7]));

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("r_key"			, "date"		, "log_number", 
									"sdata1"	, "bdata"	, "sdata2", 
									"ddata"		, "sdata3"		, "adata"));
	}

}