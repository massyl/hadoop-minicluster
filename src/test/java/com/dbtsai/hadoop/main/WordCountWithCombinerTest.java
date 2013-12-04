package com.dbtsai.hadoop.main;

import com.dbtsai.hadoop.mapreduce.WordCountMR;
import com.dbtsai.hadoop.util.HadoopMiniClusterTestBase;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 * User: dbtsai
 * Date: 12/3/13
 * Time: 1:52 PM
 * To change this template use File | Settings | File Templates.
 */
public class WordCountWithCombinerTest extends HadoopMiniClusterTestBase {
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

}
