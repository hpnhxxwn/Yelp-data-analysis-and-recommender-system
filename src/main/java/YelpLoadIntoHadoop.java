/**
 * Created by hpnhxxwn on 2017/2/14.
 */
import java.io.IOException;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.codehaus.jackson.map.ObjectMapper;
import org.apache.hadoop.util.GenericOptionsParser;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class YelpLoadIntoHadoop {
    public static class YelpReviewMapper extends Mapper<Object, Text, NullWritable, Text> {
        private ReviewWritable out = new ReviewWritable();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                ObjectMapper mapper = new ObjectMapper();
                String[] tuples = value.toString().split("\n");
                JSONObject jsn = new JSONObject();
                for (String tuple : tuples) {
                    tuple = tuple.replace("yelp_restaurant_distinct::", "");
                    tuple = tuple.replace("yelp_review_data::", "");
                    System.out.println(tuple);
                    Review review = mapper.readValue(tuple, Review.class);
                    if (review != null && review.text != null) {
                        out.setText(review.text);
                        out.setStars(review.stars);
                        if (Float.parseFloat(review.stars) > 3.0) {
                            out.setPosLabel(1);
                            out.setNegLabel(0);
                        }
                        else {
                            out.setPosLabel(0);
                            out.setNegLabel(1);
                        }
                        try {
                            jsn.put("text", review.text);
                            jsn.put("stars", review.stars);
                            jsn.put("positive label", out.getPosLabel());
                            jsn.put("negative label", out.getNegLabel());
                        }
                        catch(JSONException e2) {
                            e2.printStackTrace();
                        }
                        context.write(NullWritable.get(), new Text(jsn.toString()));
                    }

                }
            }
            catch(ArithmeticException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: AverageDriver <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Cleaning review.json file to filter out review texts, stars and labels");
        job.setJarByClass(YelpLoadIntoHadoop.class);
        job.setMapperClass(YelpReviewMapper.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        Path inPath = new Path(otherArgs[0]);
        Path outPath = new Path(otherArgs[1]);
        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        outPath.getFileSystem(conf).delete(outPath, true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
