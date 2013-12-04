package com.dbtsai.hadoop.main;

import com.dbtsai.hadoop.mapreduce.WordCountMR;
import com.dbtsai.hadoop.util.HadoopMiniClusterTestBase;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;

/**
 * Created with IntelliJ IDEA.
 * User: dbtsai
 * Date: 12/3/13
 * Time: 1:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class WordCountTest extends HadoopMiniClusterTestBase {
    private static Configuration conf;

    @BeforeClass
    public static void uploadDataSets() throws Exception {
        BufferedReader wikiJavaPageIn = new BufferedReader(new InputStreamReader(WordCountTest.class.getClass().getResourceAsStream("/com/dbtsai/hadoop/main/WikiJavaPage.txt")));
        BufferedReader wikiPythonPageIn = new BufferedReader(new InputStreamReader(WordCountTest.class.getClass().getResourceAsStream("/com/dbtsai/hadoop/main/WikiPythonPage.txt")));
        BufferedReader wikiScalaPageIn = new BufferedReader(new InputStreamReader(WordCountTest.class.getClass().getResourceAsStream("/com/dbtsai/hadoop/main/WikiScalaPage.txt")));

        FSDataOutputStream out;
        conf = getConfiguration();
        FileSystem fs = FileSystem.get(conf);

        out = fs.create(new Path("/tmp/WordCountTest/", "WikiJavaPage.txt"));
        for (String line = wikiJavaPageIn.readLine(); line != null; line = wikiJavaPageIn.readLine()) {
            out.writeBytes(line);
        }
        wikiJavaPageIn.close();
        out.close();

        out = fs.create(new Path("/tmp/WordCountTest/", "WikiPythonPage.txt"));
        for (String line = wikiPythonPageIn.readLine(); line != null; line = wikiPythonPageIn.readLine()) {
            out.writeBytes(line);
        }
        wikiPythonPageIn.close();
        out.close();

        out = fs.create(new Path("/tmp/WordCountTest/", "WikiScalaPage.txt"));
        for (String line = wikiScalaPageIn.readLine(); line != null; line = wikiScalaPageIn.readLine()) {
            out.writeBytes(line);
        }
        wikiScalaPageIn.close();
        out.close();
    }

    @Test
    public void testWordCountWithWikiJavaPageDataSet() throws Exception {
        String[] arg = new String[] {"/tmp/WordCountTest/WikiJavaPage.txt", "/tmp/WordCountTest/WikiJavaPageResult/", new Gson().toJson(conf)};
        WordCount.main(arg);
    }

    @Test
    public void testWordCountWithWikiPythonPageDataSet() throws Exception {
        String[] arg = new String[] {"/tmp/WordCountTest/WikiPythonPage.txt", "/tmp/WordCountTest/WikiPythonPageResult/", new Gson().toJson(conf)};
        WordCount.main(arg);
    }

    @Test
    public void testWordCountWithWikiScalaPageDataSet() throws Exception {
        String[] arg = new String[] {"/tmp/WordCountTest/WikiScalaPage.txt", "/tmp/WordCountTest/WikiScalaPageResult/", new Gson().toJson(conf)};
        WordCount.main(arg);
    }
}
