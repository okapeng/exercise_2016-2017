import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.stream.JsonParsingException;
import java.io.IOException;
import java.io.StringReader;

public class ScanTweetsMapperExt2 extends Mapper<LongWritable, Text, LongWritable, Text> {

    private static final String RETWEET_COUNT = "retweet_count";
    private static final String USER = "user";
    private static final String NAME = "name";

    public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {
        String line = value.toString();
        JsonReader reader = Json.createReader(new StringReader(line));

        try {
            JsonObject mainObject = reader.readObject();
            JsonObject retweetedStatus = mainObject.getJsonObject("retweeted_status");
            int retweetCount = retweetedStatus.getInt(RETWEET_COUNT);

            JsonObject user = retweetedStatus.getJsonObject(USER);
            String userName = user.getString(NAME);

            output.write(new LongWritable(retweetCount), new Text(userName));
        } catch (NullPointerException e) {
            return;
        } catch (JsonParsingException e) {
            System.out.println("Not valid Json String" + e.getMessage());
        } finally {
            reader.close();
        }
    }
}
