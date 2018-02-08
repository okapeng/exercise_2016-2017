import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

/**
 * Created by Yuqing Wang on 2017/4/15.
 */
public class W11Practical {
    //the length of the command-line arguments
    private static final int LENGTH_OF_ARGS = 2;

    /**
     * the main method of the W11Practical class.
     * @param args command-line arguments. args[0] is the input file path, args[1] is the output file path
     */
    public static void main(String[] args) {
        //check whether the command-line arguments are used correctly
        if (args.length != LENGTH_OF_ARGS) {
            System.out.println("Usage: java -cp \"lib/*:bin\" W11Practical <input_path> <output_path>");
            System.exit(1);
        }

        String input_path = args[0];
        String output_path = args[1];

        try {
            //call the countTags method in the W11Practical class to use hadoop to calculate the counts for each hash tag
            W11Practical practical = new W11Practical();
            practical.countTags(input_path, output_path);
        } catch (IOException e) {
            System.out.println("Path incorrect: " + e.getMessage());
        }

    }

    /**
     * this method sets up the map reduce job to count the occurrence of hashtags in the tweets.
     * @param input_path the input file path
     * @param output_path the output file path
     * @throws IOException when the file path is incorrect
     */
    public void countTags(String input_path, String output_path) throws IOException {
        // Setup new Job and Configuration
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "W11Practical");

        // Specify input and output paths
        FileInputFormat.setInputPaths(job, new Path(input_path));
        FileOutputFormat.setOutputPath(job, new Path(output_path));

        // Set the ScanTweetsMapper as the mapper
        job.setMapperClass(ScanTweetsMapper.class);

        // Specify output types produced by mapper (each tweet hashtag with count of 1)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // The output of the reducer is a map from unique tweet tags to their total counts.
        job.setReducerClass(CountHashTagsReducer.class);

        // Specify the output types produced by reducer (tweet tags with the number of occurrence)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //LocalJobRunner.setLocalMaxRunningMaps(job, 10);

        long startTime = 0;
        long endTime = 0;

        try {
            startTime = System.currentTimeMillis();
            job.waitForCompletion(true);
            endTime = System.currentTimeMillis();
        } catch (ClassNotFoundException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }

        //System.out.println("\n\n\n\n" + (endTime - startTime));
    }
}
