/**
 * Created by chandresh.pancholi on 7/7/15.
 */
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import twitter4j.*;
import twitter4j.auth.AccessToken;

import java.util.List;
import java.util.Map;

public class TwitterSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private String consumerKey;
    private String consumerSecreteKey;
    private String accessToken;
    private String accessTokenSecret;
    private Twitter twitter;

    public TwitterSpout() {
        this.consumerKey = "bwYLPRTD2KSQFxOXmrR0GA";
        this.consumerSecreteKey = "SAHyS5KtuMKBqe97WeGTs4r1w9guH1CnkYIvXg4kHQ";
        this.accessToken = "149225519-e0OKX5njXfHQUdu8jgshYxgD1HcNI3A9Fdadit6x";
        this.accessTokenSecret = "y6FUihJmC0lXic0K7iNrkDgHBguwmbUhizbEzxtVKkxZX";
        twitter = new TwitterFactory().getSingleton();
        twitter.setOAuthConsumer(consumerKey,consumerSecreteKey);
        twitter.setOAuthAccessToken(new AccessToken(accessToken,accessTokenSecret));
    }


    public void open( Map conf, TopologyContext context, SpoutOutputCollector collector )
    {
        this.collector = collector;
    }

    public void nextTuple() {
        try {
            Query searchQuery = new Query("#MSDhoni");
            QueryResult result;
            do {
                result = twitter.search(searchQuery);
                List<Status> tweets = result.getTweets();
                for (Status tweet : tweets) {
                    String username = tweet.getUser().getScreenName();
                    String userTweet = tweet.getText();
                    long userId = tweet.getUser().getId();
                    collector.emit(new Values(userTweet), userTweet);
                }

            } while ((searchQuery = result.nextQuery()) != null);
        } catch (TwitterException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));
    }
}
