package storm.starter.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Date;
import java.util.Map;
import java.util.Random;

public class RandomLetter extends BaseRichSpout {
	  SpoutOutputCollector _collector;
	  Random _rand;


	  @Override
	  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	    _collector = collector;
	    _rand = new Random();
	  }

	  @Override
	  public void nextTuple() {
	    Utils.sleep(100);
	    String[] sentences = new String[]{ "A","B","C","D" };
	    String sentence = sentences[_rand.nextInt(sentences.length)];
	    Date d= new Date();
	    _collector.emit(new Values(sentence,d.toString()));
	  }

	  @Override
	  public void ack(Object id) {
	  }

	  @Override
	  public void fail(Object id) {
	  }

	  @Override
	  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields("word","date"));
	  }

	}