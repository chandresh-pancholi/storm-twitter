import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * Created by chandresh.pancholi on 7/7/15.
 */
public class TwitterTopology {
    public static void main(String args[]){
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new TwitterSpout());
        builder.setBolt("bolt", new ReadTweetsBolt()).shuffleGrouping("spout");

        Config config = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", config, builder.createTopology());
        Utils.sleep(1000);
        cluster.killTopology("test");
        cluster.shutdown();

    }
}
