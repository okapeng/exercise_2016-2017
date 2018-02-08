import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Yuqing Wang on 2017/4/15.
 */
public class W11PracticalExt2 {
    //the length of the command-line argument
    private static final int LENGTH_OF_ARGS = 2;

    /**
     * the main method for the W11PracticalExt2 class.
     * @param args command-line arguments. args[0] is the input file path and args[1] is the output file path
     */
    public static void main(String[] args) {
        //check if the command-line arguments are used correctly
        if (args.length != LENGTH_OF_ARGS) {
            System.out.println("Usage: java -cp \"lib/*:bin\" W11PracticalExt2 <input_path> <output_path>");
            System.exit(1);
        }

        String input_path = args[0];
        String output_path = args[1];

        try {
            //call the findUsersWithRetweets method in the W11PracticalExt2 to establishing the users associated with the most frequently re-tweeted tweets
            W11PracticalExt2 ext2 = new W11PracticalExt2();
            ext2.findUsersWithRetweets(input_path, output_path);
        } catch (IOException e) {
            System.out.println("Path incorrect: " + e.getMessage());
        }

    }

    /**
     * the findUsersWithRetweets method set up the map reduce to establish the users associated with the most frequently re-tweeted tweets.
     * @param input_path the input file path
     * @param output_path the output file path
     * @throws IOException when the file path is incorrect
     */
    public void findUsersWithRetweets(String input_path, String output_path) throws IOException {
        // Setup new Job and Configuration
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "W11PracticalExt2");

        // Specify input and output paths
        FileInputFormat.setInputPaths(job, new Path(input_path));
        FileOutputFormat.setOutputPath(job, new Path(output_path));

        // Set the ScanReTweetMapper as the mapper
        job.setMapperClass(ScanReTweetMapper.class);

        // Specify output types produced by mapper (retweet count with the user id)
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        // The output of the reducer is a map from the username to the retweet count of that user.
        job.setReducerClass(CountRetweetsReducer.class);

        // Specify the output types produced by reducer (the username with the retweeted count of the tweet produced by that user)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        try {
            job.waitForCompletion(true);
        } catch (ClassNotFoundException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
    }
}
