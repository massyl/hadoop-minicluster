package com.dbtsai.hadoop.main;

import com.dbtsai.hadoop.util.HadoopMiniClusterTestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created with IntelliJ IDEA.
 * User: dbtsai
 * Date: 12/3/13
 * Time: 1:52 PM
 * To change this template use File | Settings | File Templates.
 */
public class WordCountWithInMapperCombinerTest extends HadoopMiniClusterTestBase {

    @Test
    public void testWordCountWithWikiJavaPageDataSet() throws Exception {
        BufferedReader wikiJavaPageIn = new BufferedReader(new InputStreamReader(WordCountTest.class.getClassLoader().getResourceAsStream("com/dbtsai/hadoop/main/WikiJavaPage.txt")));

        FSDataOutputStream out;
        Configuration conf = super.getConfiguration();
        conf.set("mapred.job.tracker", "local");
        FileSystem fs = FileSystem.get(conf);

        out = fs.create(new Path("/tmp/WordCountWithInMapperCombinerTest/", "WikiJavaPage.txt"));
        for (String line = wikiJavaPageIn.readLine(); line != null; line = wikiJavaPageIn.readLine()) {
            out.writeBytes(line + "\n");
        }
        wikiJavaPageIn.close();
        out.close();

        String[] arg = new String[]{"/tmp/WordCountWithInMapperCombinerTest/WikiJavaPage.txt", "/tmp/WordCountWithInMapperCombinerTest/WikiJavaPageResult/"};
        boolean isSuccessful = WordCountWithInMapperCombiner.main(arg, conf);

        Map<String, Long> wordCount = new HashMap<String, Long>();

        FileStatus[] status = fs.listStatus(new Path("/tmp/WordCountWithInMapperCombinerTest/WikiJavaPageResult/"));

        for (int i = 0; i < status.length; i++) {
            if (status[i].getPath().getName().contains("part-r-")) {
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));

                for (String line = br.readLine(); line != null; line = br.readLine()) {
                    String temp[] = line.split("\t");
                    wordCount.put(temp[0], Long.parseLong(temp[1]));
                }
            }
        }

        for (Map.Entry<String, Long> entry : wordCount.entrySet()) {
            System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }

        assertTrue(isSuccessful);
        assertEquals("Key = for, Value = 3", 3, wordCount.get("for").intValue());
        assertEquals("Key = developed, Value = 3", 3, wordCount.get("developed").intValue());
        assertEquals("Key = is, Value = 4", 4, wordCount.get("is").intValue());
        assertEquals("Key = as, Value = 6", 6, wordCount.get("as").intValue());
        assertEquals("Key = the, Value = 6", 6, wordCount.get("the").intValue());
        assertEquals("Key = in, Value = 4", 4, wordCount.get("in").intValue());
        assertEquals("Key = java, Value = 10", 10, wordCount.get("java").intValue());
    }

    @Test
    public void testWordCountWithWikiPythonPageDataSet() throws Exception {
        BufferedReader wikiPythonPageIn = new BufferedReader(new InputStreamReader(WordCountTest.class.getClassLoader().getResourceAsStream("com/dbtsai/hadoop/main/WikiPythonPage.txt")));

        FSDataOutputStream out;
        Configuration conf = super.getConfiguration();
        conf.set("mapred.job.tracker", "local");
        FileSystem fs = FileSystem.get(conf);

        out = fs.create(new Path("/tmp/WordCountWithInMapperCombinerTest/", "WikiPythonPage.txt"));
        for (String line = wikiPythonPageIn.readLine(); line != null; line = wikiPythonPageIn.readLine()) {
            out.writeBytes(line + "\n");
        }
        wikiPythonPageIn.close();
        out.close();

        String[] arg = new String[]{"/tmp/WordCountWithInMapperCombinerTest/WikiPythonPage.txt", "/tmp/WordCountWithInMapperCombinerTest/WikiPythonPageResult/"};
        boolean isSuccessful = WordCountWithInMapperCombiner.main(arg, conf);

        Map<String, Long> wordCount = new HashMap<String, Long>();

        FileStatus[] status = fs.listStatus(new Path("/tmp/WordCountWithInMapperCombinerTest/WikiPythonPageResult/"));

        for (int i = 0; i < status.length; i++) {
            if (status[i].getPath().getName().contains("part-r-")) {
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));

                for (String line = br.readLine(); line != null; line = br.readLine()) {
                    String temp[] = line.split("\t");
                    wordCount.put(temp[0], Long.parseLong(temp[1]));
                }
            }
        }

        for (Map.Entry<String, Long> entry : wordCount.entrySet()) {
            System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }

        assertTrue(isSuccessful);
        assertEquals("Key = implementations, Value = 1", 1, wordCount.get("implementations").intValue());
        assertEquals("Key = its, Value = 3", 3, wordCount.get("its").intValue());
        assertEquals("Key = code, Value = 2", 2, wordCount.get("code").intValue());
        assertEquals("Key = programming, Value = 3", 3, wordCount.get("programming").intValue());
        assertEquals("Key = a, Value = 6", 6, wordCount.get("a").intValue());
        assertEquals("Key = to, Value = 2", 2, wordCount.get("to").intValue());
        assertEquals("Key = and, Value = 8", 8, wordCount.get("and").intValue());
    }

    @Test
    public void testWordCountWithWikiScalaPageDataSet() throws Exception {
        BufferedReader wikiScalaPageIn = new BufferedReader(new InputStreamReader(WordCountTest.class.getClassLoader().getResourceAsStream("com/dbtsai/hadoop/main/WikiScalaPage.txt")));

        FSDataOutputStream out;
        Configuration conf = super.getConfiguration();
        conf.set("mapred.job.tracker", "local");
        FileSystem fs = FileSystem.get(conf);

        out = fs.create(new Path("/tmp/WordCountWithInMapperCombinerTest/", "WikiScalaPage.txt"));
        for (String line = wikiScalaPageIn.readLine(); line != null; line = wikiScalaPageIn.readLine()) {
            out.writeBytes(line + "\n");
        }
        wikiScalaPageIn.close();
        out.close();

        String[] arg = new String[]{"/tmp/WordCountWithInMapperCombinerTest/WikiScalaPage.txt", "/tmp/WordCountWithInMapperCombinerTest/WikiScalaPageResult/"};
        boolean isSuccessful = WordCountWithInMapperCombiner.main(arg, conf);

        Map<String, Long> wordCount = new HashMap<String, Long>();

        FileStatus[] status = fs.listStatus(new Path("/tmp/WordCountWithInMapperCombinerTest/WikiScalaPageResult/"));

        for (int i = 0; i < status.length; i++) {
            if (status[i].getPath().getName().contains("part-r-")) {
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));

                for (String line = br.readLine(); line != null; line = br.readLine()) {
                    String temp[] = line.split("\t");
                    wordCount.put(temp[0], Long.parseLong(temp[1]));
                }
            }
        }

        for (Map.Entry<String, Long> entry : wordCount.entrySet()) {
            System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }

        assertTrue(isSuccessful);
        assertEquals("Key = system, Value = 2", 2, wordCount.get("system").intValue());
        assertEquals("Key = low, Value = 1", 1, wordCount.get("low").intValue());
        assertEquals("Key = jvm, Value = 1", 1, wordCount.get("jvm").intValue());
        assertEquals("Key = subclasses, Value = 1", 1, wordCount.get("subclasses").intValue());
        assertEquals("Key = including, Value = 4", 4, wordCount.get("including").intValue());
        assertEquals("Key = bytecode, Value = 1", 1, wordCount.get("bytecode").intValue());
        assertEquals("Key = the, Value = 7", 7, wordCount.get("the").intValue());
    }
}
