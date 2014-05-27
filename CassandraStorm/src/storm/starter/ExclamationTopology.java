package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.starter.spout.*;

import java.util.Date;
import java.util.Map;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * This is a basic example of a Storm topology.
 */
public class ExclamationTopology {

  public static class ExclamationBolt extends BaseRichBolt {
    OutputCollector _collector;
    String ComponentId;
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
      ComponentId=context.getThisComponentId();
    }

    @Override
    public void execute(Tuple tuple) {
    Date d=new Date();
      
      _collector.emit(tuple, new Values(tuple.getString(0) + "!! "+ComponentId,d.toString()));
      _collector.ack(tuple);
    }
  
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word","date"));
    }
  }
    public static class SaverBolt extends BaseRichBolt {
        OutputCollector _collector;
        String ComponentId;
        public static java.util.UUID getTimeUUID()
        {
                return java.util.UUID.fromString(new com.eaio.uuid.UUID().toString());
        }
       
        Cluster cluster;
        Session session;
/*    keyspace for the stormsync    
CREATE KEYSPACE keyspace2 WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1};
use keyspace2;

CREATE TABLE StormSync (
minute varchar,
processtime varchar,
interaction_time timeuuid,
Value varchar,
SaverId varChar,
PRIMARY KEY (minute,interaction_time)
) with CLUSTERING ORDER BY (interaction_time DESC);
        */
        
        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        	 
        	//cluster = Cluster.builder().addContactPoint("192.168.2.10").build(); //vagrant cassandra cluster
        	cluster = Cluster.builder().addContactPoint("127.0.0.1").build(); //vagrant cassandra cluster
       	     

        	 session = cluster.connect();
        	_collector = collector;
        	ComponentId=context.getThisComponentId();
        }

        @Override
        public void execute(Tuple tuple) {
       
          Date dDate=new Date();
          java.util.UUID uuid= getTimeUUID();
          String Value =tuple.getString(0) ;
          String d=tuple.getString(1);
          if (d==null)
        	  d="no time";
          String CQL="insert into Keyspace2.StormSync (minute,processtime,interaction_time,Value,saverid)"
          		+ "Values ('"+dDate.toString()+"','"+d+"',"+uuid+",'"+Value+"','"+ComponentId+"')";
          //System.out.println("CQL  "+CQL);
          session.execute(CQL);
          //cluster.shutdown();
        
          _collector.ack(tuple);
        }
        @Override
        public void cleanup(){
        	
        	//cluster.shutdown();
        	cluster.close();
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
          declarer.declare(new Fields("word"));
        }
    

  }

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();
    
    builder.setSpout("word", new mySpout(), 10);
    builder.setSpout("sentence", new RandomSentenceSpout(), 10);
    builder.setBolt("exclaim1", new ExclamationBolt(), 3).shuffleGrouping("sentence");
     builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping("word");
     builder.setBolt("exclaim3", new ExclamationBolt(), 2).shuffleGrouping("exclaim2");
     builder.setBolt("Saver", new SaverBolt(), 4).shuffleGrouping("exclaim1");
     builder.setBolt("Saver2", new SaverBolt(), 4).shuffleGrouping("exclaim3");
     builder.setBolt("Saver3", new SaverBolt(), 4).shuffleGrouping("exclaim2").shuffleGrouping("exclaim1");
    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    else {

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("test", conf, builder.createTopology());
      Utils.sleep(10000);
      cluster.killTopology("test");
      cluster.shutdown();
      
  }
  }
  
  
  //builder.setSpout("word", new TestWordSpout(), 10);
  //builder.setSpout("sentence", new RandomSentenceSpout(), 10);
  //builder.setBolt("exclaim1", new ExclamationBolt(), 3).shuffleGrouping("sentence");
  // builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping("word");
  // builder.setBolt("exclaim3", new ExclamationBolt(), 2).shuffleGrouping("exclaim2");
  // builder.setBolt("Saver", new SaverBolt(), 4).shuffleGrouping("exclaim1");
  // builder.setBolt("Saver2", new SaverBolt(), 4).shuffleGrouping("exclaim3");
}
