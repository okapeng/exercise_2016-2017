import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Yuqing Wang on 2017/4/15.
 */
public class OrderHashTagsReducer extends Reducer<LongWritable, Text, Text, LongWritable> {

    /**
     * this method overrides the reduce method in the Reducer class so that it can produce a map with each hashtag to its count of occurrence.
     * @param key the count of occurrence for a hashtag
     * @param values the text of that hash tag
     * @param output a context object that outputs the result of the Reducer
     * @throws IOException when the program fails to produce output
     * @throws InterruptedException when the program fails to produce output
     */
    public void reduce(LongWritable key, Iterable<Text> values, Context output) throws IOException, InterruptedException {

        //swap the value(hash tag) and key(count of occurrence)
        for (Text value : values) {
            output.write(value, key);
        }
    }
}
