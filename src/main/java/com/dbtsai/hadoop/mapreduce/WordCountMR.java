package com.dbtsai.hadoop.mapreduce;

import com.dbtsai.hadoop.util.CombiningFunction;
import com.dbtsai.hadoop.util.InMapperCombiner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;


public class WordCountMR {

    static String getCleanLine(String line) {
        line = line.replaceAll("[^A-Za-z]", " ").toLowerCase();
        return line;
    }

    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final static LongWritable one = new LongWritable(1);
        private final Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = getCleanLine(value.toString());
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class WordCountMapperWithInMapperCombiner extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final static LongWritable one = new LongWritable(1);
        private final Text word = new Text();
        private final InMapperCombiner combiner = new InMapperCombiner<Text, LongWritable>(8388608,
                new CombiningFunction<LongWritable>() {
                    @Override
                    public LongWritable combine(LongWritable value1, LongWritable value2) {
                        value1.set(value1.get() + value2.get());
                        return value1;
                    }
                }
        );

        @Override
        @SuppressWarnings("unchecked")
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = getCleanLine(value.toString());
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                combiner.write(word, one, context);
            }
        }

        @Override
        protected void cleanup(Mapper.Context context) throws IOException, InterruptedException {
            combiner.flush(context);
        }
    }

    public static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        static long minReducerOutput;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            minReducerOutput = Long.parseLong(context.getConfiguration().get("minReducerOutput", "0"));
        }

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            if (sum > minReducerOutput) {
                context.write(key, new LongWritable(sum));
            }
        }
    }

    public static class WordCountCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }
}
