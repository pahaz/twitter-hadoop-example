import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class TwitterAvgFollowers {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, FloatWritable> {
    private final static IntWritable static_key = new IntWritable(0);
    private FloatWritable val = new FloatWritable();

    public void map(LongWritable key, Text value, OutputCollector<IntWritable, FloatWritable> output, Reporter reporter) throws IOException {
      String line = value.toString();
      String[] twitter_id_and_count_followers = line.split("\t");
      if (twitter_id_and_count_followers.length == 2) {
        Float _count_followers = Float.parseFloat(twitter_id_and_count_followers[1]);
        val.set(_count_followers);
        output.collect(static_key, val);
      }
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {
    private FloatWritable val = new FloatWritable();

    public void reduce(IntWritable key, Iterator<FloatWritable> values, OutputCollector<IntWritable, FloatWritable> output, Reporter reporter) throws IOException {
      Float sum = 0f;
      Integer n = 0;
      while (values.hasNext()) {
        Float avg_followers = values.next().get();
        sum += avg_followers;
        n += 1;
      }

      val.set(sum / n);
      output.collect(key, val);
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(TwitterAvgFollowers.class);
    conf.setJobName("TwitterAvgFollowers");

    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(FloatWritable.class);

    conf.setMapperClass(Map.class);
    //conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }
}
