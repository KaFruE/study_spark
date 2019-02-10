package bigdata.storm;

import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;


public class EsperBolt extends BaseBasicBolt{

	private static final long serialVersionUID = 1L;

	private EPServiceProvider espService;

	private boolean isOverSpeedEvent = false;

	public void prepare(Map stormConf, TopologyContext context) {

		Configuration configuration = new Configuration();
		configuration.addEventType("LogInfoBean", LogInfoBean.class.getName());

		espService = EPServiceProviderManager.getDefaultProvider(configuration);
		espService.initialize();
		
		int avgOverData = 30;
		int windowTime  = 30;
		
		String overRuleEpl =  "SELECT date, logNumber, sdata1, bdata, "
								+ "sdata2, ddata, sdata3 , adata "
								+ " FROM LogInfoBean.win:time_batch("+windowTime+" sec) "
								+ " GROUP BY logNumber HAVING AVG(sdata3) > " + avgOverData;

		EPStatement loginfoStmt = espService.getEPAdministrator().createEPL(overRuleEpl);

		loginfoStmt.addListener((UpdateListener) new OverRuleEventListener());
	}


	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {

		// TODO Auto-generated method stub
		String tValue = tuple.getString(0); 

		String[] receiveData = tValue.split("\\,");

		LogInfoBean driverCarInfoBean =new LogInfoBean();

		driverCarInfoBean.setDate(receiveData[0]);
		driverCarInfoBean.setLogNumber(receiveData[1]);
		driverCarInfoBean.setSdata1(receiveData[2]);
		driverCarInfoBean.setBdata(receiveData[3]);
		driverCarInfoBean.setSdata2(receiveData[4]);
		driverCarInfoBean.setDdata(receiveData[5]);
		driverCarInfoBean.setSdata3(Integer.parseInt(receiveData[6]));
		driverCarInfoBean.setAdata(receiveData[7]);

		espService.getEPRuntime().sendEvent(driverCarInfoBean); 


		if(isOverSpeedEvent) {
			collector.emit(new Values(	driverCarInfoBean.getDate().substring(0,8), 
										driverCarInfoBean.getLogNumber()+"-"+driverCarInfoBean.getDate()));
			isOverSpeedEvent = false;
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("date", "log_number"));
	}


	private class OverRuleEventListener implements UpdateListener
	{
		@Override
		public void update(EventBean[] newEvents, EventBean[] oldEvents) {
			if (newEvents != null) {
				try {
					isOverSpeedEvent = true;
				} catch (Exception e) {
					System.out.println("Failed to Listener Update" + e);
				} 
			}
		}
	}

}