import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.stream.JsonParsingException;
import java.io.StringReader;

/**
 * Created by Yuqing Wang on 2017/4/15.
 */
public class ScanTweetsMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    //use final Strings and int to avoid magic strings and constants.
    //field's name in the input Json string
    private static final String ENTITIES = "entities";
    private static final String HASHTAGS = "hashtags";
    private static final String TEXT = "text";
    private static final int COUNT = 1;

    /**
     * this method overrides the map method in the Mapper class so that it can map each hash tag with 1.
     * @param key a key automatically generate by hadoop
     * @param value the input json string
     * @param output a Context object that outputs the result of the map class
     * @throws IOException when the program fails to produce output
     * @throws InterruptedException when the program fails to produce output
     */
    public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {
        //transfer the input text to string and create a JsonReader object to read  the input json string
        String line = value.toString();
        JsonReader reader = Json.createReader(new StringReader(line));
        try {
            //read the main object of the json string and the object entities which contain details of hash tags
            JsonObject mainObject = reader.readObject();
            JsonObject entities = mainObject.getJsonObject(ENTITIES);
            //call the checkJsonArray method in the Checker class to make sure the JsonArray is valid (not null or empty)
            Checker.checkJsonArray(entities, HASHTAGS);
            JsonArray hashtags = entities.getJsonArray(HASHTAGS);

            //go through the array and map each hashtag with the count of 1
            for (int i = 0; i < hashtags.size(); i++) {
                JsonObject tag = hashtags.getJsonObject(i);

                //make sure the String is not empty or null
                Checker.checkJsonString(tag, TEXT);
                String tagText = tag.getString(TEXT);

                //map each hash tag with the count of 1
                output.write(new Text(tagText), new LongWritable(COUNT));
            }

        } catch (NullPointerException e) {
            //when the field or array is not valid, a NullPointerException will be thrown and this field or array will be ignore
        } catch (JsonParsingException e) {
            System.out.println("Not valid Json String! " + e.getMessage());
        } finally {
            reader.close();
        }
    }
}
