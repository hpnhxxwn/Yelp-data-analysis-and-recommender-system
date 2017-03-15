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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class YelpLoadIntoHadoop extends Configured implements Tool  {
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "aggprog");
        job.setJarByClass(YelpLoadIntoHadoop.class);
        MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,YelpBusinessMapper.class);
        MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,YelpReviewMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setReducerClass(YelpReviewReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        return (job.waitForCompletion(true) ? 0 : 1);
    }


    public static class YelpBusinessMapper extends Mapper<Object, Text, Text, Text> {
        //private BusinessWritable out = new BusinessWritable();
        private Text k = new Text();
        @Override
        public void map(Object key, Text value, Context context) {
            try {
                ObjectMapper mapper = new ObjectMapper();
                //System.out.println("Business Mapper");

                //String[] tuples = value.toString().split("\n");
                //for (String tuple: tuples) {
                String tuple = value.toString().replace("\\\\n", "");
                //System.out.println(value.toString());
                Business business = mapper.readValue(value.toString(), Business.class);
                    //out.setBusinessId(business.business_id);
                k.set(business.business_id);
                //System.out.println("DEBUG");
                //System.out.println(business.business_id);
                boolean food = false;
                if (business.categories != null) {
                    for (String str: business.categories) {
                        if (str.contains("Food") || str.contains("Restaurant") || str.contains("food") || str.contains("restaurant")){
                            food = true;
                            break;
                        }

                    }
                }

                if (!food)
                    return;
                context.write(k, new Text("B" + tuple));
                //}

            }
            catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }

    public static class YelpReviewMapper extends Mapper<Object, Text, Text, Text> {
        private Text k = new Text();
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            ObjectMapper mapper = new ObjectMapper();
            String[] tuples = value.toString().split("\n");
            //for (String tuple: tuples) {
            String tuple = value.toString().replace("\\\\n", "");
            Review review = mapper.readValue(value.toString(), Review.class);
                //System.out.println("Review Mapper");
                //System.out.println(review.stars);
            k.set(review.business_id);
            //System.out.println(review.business_id);
                context.write(k, new Text("R" + tuple));
            //}

        }

    }
    public static class YelpReviewReducer extends Reducer<Text, Text, NullWritable, Text> {
        //private ReviewWritable out = new ReviewWritable();
        private List<Text> listB = new ArrayList<Text>();
        private List<Text> listR = new ArrayList<Text>();
        private MultipleOutputs multipleOutputs;
        int c = 0;
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs(context);
        }
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            try {
                //System.out.println("Reducer Outside Loop");
                System.out.println(key.toString());
                listB.clear();
                listR.clear();
                for (Text t: values) {
                    //System.out.println("Reducer For Loop");
                    if (t.charAt(0) == 'B') {
                        //System.out.println("Business List");
                        //System.out.println(t.toString().substring(1));
                        listB.add(new Text(t.toString().substring(1)));
                    }
                    else if (t.charAt(0) == 'R') {
                        //System.out.println("Review List");
                        //System.out.println(t.toString().substring(1));
                        listR.add(new Text(t.toString().substring(1)));
                    }
                }
                //System.out.println("listB size is " + listB.size());
                //System.out.println("listR size is " + listR.size());
                executeLeftJoin(context);
            }
            catch(ArithmeticException e) {
                e.printStackTrace();
            }

        }

        public void executeLeftJoin(Context context){
            ObjectMapper mapper = new ObjectMapper();
            //String[] tuples = value.toString().split("\n");
            JSONObject jsn = new JSONObject();
            int poslabel = 0;
            int neglabel = 0;
            String RString = null;
            String BString = null;
            try {
                for (Text B : this.listB) {

                    for (Text R : this.listR) {
                        //System.out.println("DEBUG");
                        RString = R.toString();
                        BString = B.toString();
                        Review review = mapper.readValue(R.toString(), Review.class);
                        Business business = mapper.readValue(B.toString(), Business.class);
                        //System.out.println(B.toString() + ", " + R.toString());
                        if (review == null || review.text == null)
                            continue;
                        //System.out.println(review.stars);
                        if (Float.parseFloat(review.stars) > 3.0) {
                            poslabel = 1;
                            neglabel = 0;
                        } else {
                            poslabel = 0;
                            neglabel = 1;
                        }
                        try {
                            jsn.put("text", review.text);
                            jsn.put("stars", review.stars);
                            //jsn.put("user_id", review.user_id);
                            //jsn.put("business_id", review.business_id);
                            jsn.put("positive label", poslabel);
                            jsn.put("negative label", neglabel);
                        } catch (JSONException e) {

                            e.printStackTrace();
                        }
                        //System.out.println(jsn.toString());
                        multipleOutputs.write(NullWritable.get(), new Text(jsn.toString()), "sentimental-analysis");
                        multipleOutputs.write(NullWritable.get(), new Text(review.user_id + " " + review.business_id + " " + review.stars), "recommender-system");
                        multipleOutputs.write(NullWritable.get(), new Text(c + "|" + business.name), "business");
                        c++;
                    }
                }


            }
            catch (IOException e) {
                System.out.println(RString);
                System.out.println(BString);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

            @Override
            protected void cleanup(Context context)
                    throws IOException, InterruptedException {
                multipleOutputs.close();
            }
        }


    public static void main(String[] args) throws Exception{
        int ecode = ToolRunner.run(new YelpLoadIntoHadoop(), args);
        System.exit(ecode);
    }
}
