package com.dbtsai.hadoop.mapreduce;

/**
 * See Hadoop, The Definitive Guide, page 154
 * and https://cwiki.apache.org/MRUNIT/mrunit-tutorial.html
 * to know more about MRUnit
 */

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class WordCountMRTest {

    @Test
    public void testWordCountMapper() throws Exception {
        MapDriver<LongWritable, Text, Text, LongWritable> mapDriver = new MapDriver<LongWritable, Text, Text, LongWritable>();
        mapDriver.setMapper(new WordCountMR.WordCountMapper());

        // The ordering does matter, and it's the same as the emitting ordering.
        mapDriver.withInput(new LongWritable(1), new Text("apple walnut avocado avocado walnut apple avocado"));

        mapDriver.withOutput(new Text("apple"), new LongWritable(1));
        mapDriver.withOutput(new Text("walnut"), new LongWritable(1));
        mapDriver.withOutput(new Text("avocado"), new LongWritable(1));
        mapDriver.withOutput(new Text("avocado"), new LongWritable(1));
        mapDriver.withOutput(new Text("walnut"), new LongWritable(1));
        mapDriver.withOutput(new Text("apple"), new LongWritable(1));
        mapDriver.withOutput(new Text("avocado"), new LongWritable(1));

        mapDriver.runTest();
    }

    @Test
    public void testWordCountMapperWithInMapperCombiner() throws Exception {
        MapDriver<LongWritable, Text, Text, LongWritable> mapDriver = new MapDriver<LongWritable, Text, Text, LongWritable>();
        mapDriver.setMapper(new WordCountMR.WordCountMapperWithInMapperCombiner());

        mapDriver.withInput(new LongWritable(1), new Text("apple walnut avocado avocado walnut apple avocado"));

        mapDriver.withOutput(new Text("walnut"), new LongWritable(2));
        mapDriver.withOutput(new Text("apple"), new LongWritable(2));
        mapDriver.withOutput(new Text("avocado"), new LongWritable(3));

        mapDriver.runTest();
    }

    @Test
    public void testWordCountReducer() throws Exception {
        ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver = new ReduceDriver<Text, LongWritable, Text, LongWritable>();
        reduceDriver.setReducer(new WordCountMR.WordCountReducer());

        List<LongWritable> values = new ArrayList<LongWritable>();
        values.add(new LongWritable(3));
        values.add(new LongWritable(7));
        values.add(new LongWritable(11));

        reduceDriver.withInput(new Text("apple"), values);

        reduceDriver.withOutput(new Text("apple"), new LongWritable(21));

        reduceDriver.runTest();
    }

    @Test
    public void testWordCountMR() throws Exception {
        MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable>();
        mapReduceDriver.setMapper(new WordCountMR.WordCountMapper());
        mapReduceDriver.setReducer(new WordCountMR.WordCountReducer());
        mapReduceDriver.setCombiner(new WordCountMR.WordCountReducer());

        mapReduceDriver.withInput(new LongWritable(1), new Text("apple walnut avocado avocado walnut apple avocado"));

        // Notice that the key is sorted in the following example.
        mapReduceDriver.withOutput(new Text("apple"), new LongWritable(2));
        mapReduceDriver.withOutput(new Text("avocado"), new LongWritable(3));
        mapReduceDriver.withOutput(new Text("walnut"), new LongWritable(2));

        mapReduceDriver.runTest();
    }

    @Test
    public void testWordCountMRWithInMapperCombiner() throws Exception {
        MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable>();
        mapReduceDriver.setMapper(new WordCountMR.WordCountMapperWithInMapperCombiner());
        mapReduceDriver.setReducer(new WordCountMR.WordCountReducer());
        mapReduceDriver.setCombiner(new WordCountMR.WordCountReducer());

        mapReduceDriver.withInput(new LongWritable(1), new Text("apple walnut avocado avocado walnut apple avocado"));

        // Notice that the key is sorted in the following example.
        mapReduceDriver.withOutput(new Text("apple"), new LongWritable(2));
        mapReduceDriver.withOutput(new Text("avocado"), new LongWritable(3));
        mapReduceDriver.withOutput(new Text("walnut"), new LongWritable(2));

        mapReduceDriver.runTest();
    }
}