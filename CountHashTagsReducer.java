import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by Yuqing Wang on 2017/4/15.
 */
public class CountHashTagsReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    /**
     * this method overrides the reduce method in the Reducer class so that it can add the count of each hashtag together to get the total count.
     * @param key the text of hash tag
     * @param values a list of count(1)
     * @param output a Context object that outputs the result of the reducer
     * @throws IOException when the program fails to produce output
     * @throws InterruptedException when the program fails to produce output
     */
    public void reduce(Text key, Iterable<LongWritable> values, Context output) throws IOException, InterruptedException {
        int sum = 0;
        //go through the list of counts and add them together to get the total count
        for (LongWritable value : values) {
            long l = value.get();
            sum += l;
        }

        output.write(key, new LongWritable(sum));
    }
}
