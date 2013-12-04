package com.dbtsai.hadoop.main;

import com.dbtsai.hadoop.mapreduce.WordCountMR;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Created with IntelliJ IDEA.
 * User: dbtsai
 * Date: 11/11/13
 * Time: 5:41 PM
 * To change this template use File | Settings | File Templates.
 */
public class WordCountWithCombiner {
    public static void main(String[] args) throws Exception {
        Configuration conf;
        if (args[2] == null) {
            conf = new Configuration();
        } else {
            Gson gson = new Gson();
            conf = gson.fromJson(args[2], Configuration.class);
        }        // Ignore the worlds appeared less than 1000 times.
        // conf.set("minReducerOutput", "1000");

        Job job = new Job(conf, "Word Count With Combiner");
        job.setJarByClass(WordCountWithCombiner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(WordCountMR.WordCountMapper.class);
        job.setCombinerClass(WordCountMR.WordCountCombiner.class);
        job.setReducerClass(WordCountMR.WordCountReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(16);

        job.waitForCompletion(true);
    }
}
