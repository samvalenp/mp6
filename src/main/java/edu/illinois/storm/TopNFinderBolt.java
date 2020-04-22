package edu.illinois.storm;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.TreeSet;

import static org.apache.storm.utils.Utils.tuple;

/** a bolt that finds the top n words. */
public class TopNFinderBolt extends BaseRichBolt {
  private OutputCollector collector;
  private TreeSet<Pair<Integer, String>> top = new TreeSet<Pair<Integer, String>>();
  private int limit;
  // Hint: Add necessary instance variables and inner classes if needed


  @Override
  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
  }

  public TopNFinderBolt withNProperties(int n) {
    /* ----------------------TODO-----------------------
    Task: set N
    ------------------------------------------------- */
    this.limit = n;
		// End
		return this;
  }

  @Override
  public void execute(Tuple tuple) {
    /* ----------------------TODO-----------------------
    Task: keep track of the top N words
		Hint: implement efficient algorithm so that it won't be shutdown before task finished
		      the algorithm we used when we developed the auto-grader is maintaining a N size min-heap
    ------------------------------------------------- */
    String word = (String) tuple.getValues().get(0);
    int count = Integer.parseInt((String)tuple.getValues().get(1));
    System.out.println("LOCOOOOOOOOOOOOO   " + word + " " + count);

    top.add(new Pair<Integer, String>(count, word));
    if(top.size() > this.limit){
      top.remove(top.first());
    }

    StringBuilder sb = new StringBuilder();
    for (Pair<Integer, String> item : top) {
      System.out.println("estaaaas   " + item);
      sb.append(item.second + ", ");
    }

    collector.emit(tuple("top-N", sb.substring(0, sb.length() - 2)));
		// End
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    /* ----------------------TODO-----------------------
    Task: define output fields
		Hint: there's no requirement on sequence;
					For example, for top 3 words set ("hello", "word", "cs498"),
					"hello, world, cs498" and "world, cs498, hello" are all correct
    ------------------------------------------------- */
    declarer.declare(new Fields("top-N", "words"));
    // END
  }

}

class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
