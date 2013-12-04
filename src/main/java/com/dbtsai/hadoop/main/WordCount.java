package com.dbtsai.hadoop.main;

import com.dbtsai.hadoop.mapreduce.WordCountMR;
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
 * Time: 5:40 PM
 * To change this template use File | Settings | File Templates.
 */
public class WordCount {

    public static boolean main(String[] args) throws Exception {
        return main(args, new Configuration());
    }

    public static boolean main(String[] args, Configuration conf) throws Exception {
        // Ignore the worlds appeared less than 1000 times.
        // conf.set("minReducerOutput", "1000");

        Job job = new Job(conf, "Word Count");
        job.setJarByClass(WordCount.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(WordCountMR.WordCountMapper.class);
        job.setReducerClass(WordCountMR.WordCountReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(2);

        job.waitForCompletion(true);
        return job.isSuccessful();
    }
}
