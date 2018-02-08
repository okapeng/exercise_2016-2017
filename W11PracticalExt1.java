import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * Created by Yuqing Wang on 2017/4/15.
 */
public class W11PracticalExt1 {
    //use the final strings and int to avoid magic strings and constant.
    private static final String TEMP_OUTPUT_PATH = "temp_output"; // the path of the temp output folder
    private static final String TRUE = "true";
    private static final String FALSE = "false";
    private static final int LENGTH_OF_ARGS = 3; //the length of the command-line argument

    /**
     * the main method for the W11PracticalExt1 class.
     * @param args command-line arguments. args[0] is the input file path, args[1] is the output file path and args[2] reflects whether the user wants to order the hashtags by popularity
     */
    public static void main(String[] args) {
        //check if the command-line arguments are used correctly
        if (args.length != LENGTH_OF_ARGS) {
            System.out.println("Usage: java -cp \"lib/*:bin\" W11PracticalExt1 <input_path> <output_path> <if_order_by_popularity>");
            System.exit(1);
        }

        try {
            //call the switchCommand method in the W11PracticalExt1 class to identify which task the user wants the program to carry out
            W11PracticalExt1 ext1 = new W11PracticalExt1();
            ext1.switchCommand(args[0], args[1], args[2]);
        } catch (IOException e) {
            System.out.println("Path incorrect: " + e.getMessage());
        }

    }

    /**
     * this method calls different method based on the user's decision on whether order the hashtags by occurrence frequency.
     * @param input_path the input file path
     * @param output_path the output file path
     * @param ifOrderByPopularity command from the user
     * @throws IOException when the file path is incorrect
     */
    public void switchCommand(String input_path, String output_path, String ifOrderByPopularity) throws IOException {

        W11Practical practical = new W11Practical();
        W11PracticalExt1 ext1 = new W11PracticalExt1();

        switch (ifOrderByPopularity) {
            case TRUE:
                /*
                if the user wants to order the hashtags by occurrence frequency, the program will first call the countTags method
                in the W11Practical class to count the occurrence of each hashtag and output the result to a temp-output folder
                Then, the program will call the orderByPopularity method in the W11PracticalExt1 class to order the tags by frequency and
                output the result to the given output path
                */
                practical.countTags(input_path, TEMP_OUTPUT_PATH);
                ext1.orderByPopularity(TEMP_OUTPUT_PATH, output_path);

                //after the program finishing ordering the hashtag, the temp_output folder will be delete
                FileUtils.deleteDirectory(new File(TEMP_OUTPUT_PATH));

            case FALSE:
                //if the user doesn't want to order by frequency, the program will run the same code as the original practical
                practical.countTags(input_path, output_path);

            default:
                System.out.println("Usage: java -cp \"lib/*:bin\" W11PracticalExt1 <input_path> <output_path> <if_order_by_popularity>");
        }
    }

    /**
     * this method sets up the map reduce job to order the hashtags by popularity.
     * @param input_path the input file path (in this case, it is the temp_output)
     * @param output_path the output file path
     * @throws IOException when the file path is incorrect
     */
    public void orderByPopularity(String input_path, String output_path) throws IOException {
        // Setup new Job and Configuration
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "W11PracticalExt1");

        // Specify input and output paths
        FileInputFormat.setInputPaths(job, new Path(input_path));
        FileOutputFormat.setOutputPath(job, new Path(output_path));

        // Set the OrderHashTagsMapper as the mapper
        job.setMapperClass(OrderHashTagsMapper.class);

        // Specify output types produced by mapper (the count with each hash tag )
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        // The output of the reducer is a map from hashtag to its total count ordering by the occurrence frequency.
        job.setReducerClass(OrderHashTagsReducer.class);

        // Specify the output types produced by reducer
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
