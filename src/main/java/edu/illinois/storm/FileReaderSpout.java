package edu.illinois.storm;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import java.util.Scanner;

/** a spout that generate sentences from a file */
public class FileReaderSpout implements IRichSpout {
  private SpoutOutputCollector _collector;
  private TopologyContext _context;
  private String inputFile;
  private Scanner sc;

  // Hint: Add necessary instance variables if needed

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    this._context = context;
    this._collector = collector;
    this.sc = new Scanner(new File("inputFile"));
    /* ----------------------TODO-----------------------
    Task: initialize the file reader
    ------------------------------------------------- */

    // END

  }

  // Set input file path
  public FileReaderSpout withInputFileProperties(String inputFile) {
    this.inputFile = inputFile;
    return this;
  }

  @Override
  public void nextTuple() {
    if(sc.hasNextLine()){
      String line = sc.nextLine();
      _collector.emit(new Values(line));
    }else{
      Utils.sleep(1000);
    }
    /* ----------------------TODO-----------------------
    Task:
    1. read the next line and emit a tuple for it
    2. don't forget to add a small sleep when the file is entirely read to prevent a busy-loop
    ------------------------------------------------- */
    // END
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    /* ----------------------TODO-----------------------
    Task: define the declarer
    ------------------------------------------------- */
    declarer.declare(new Fields("line"));
    // END
  }

  @Override
  public void close() {
    /* ----------------------TODO-----------------------
    Task: close the file
    ------------------------------------------------- */
    sc.close();
    // END

  }

  public void fail(Object msgId) {}

  public void ack(Object msgId) {}

  @Override
  public void activate() {}

  @Override
  public void deactivate() {}

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
