import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Yuqing Wang on 2017/4/15.
 */
public class CountRetweetsReducer extends Reducer<LongWritable, Text, Text, LongWritable> {

    /**
     * this method overrides the one in the Reducer class so that it can produce a map with the users to the retweeted count.
     * @param key the retweeted count
     * @param values a list of username of authors who post tweet with a specific number of count
     * @param output a Context object that output the result of the reducer
     * @throws IOException when the program fails to produce output
     * @throws InterruptedException when the program fails to produce output
     */
    public void reduce(LongWritable key, Iterable<Text> values, Context output) throws IOException, InterruptedException {
        //go through the list of username and swap the value and key
        for (Text value : values) {
            output.write(value, key);
        }
    }
}
