package com.dbtsai.hadoop.main;

import com.dbtsai.hadoop.util.HadoopMiniClusterTestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
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
        String[] arg = new String[]{"/tmp/WordCountTest/WikiJavaPage.txt", "/tmp/WordCountTest/WikiJavaPageResult/"};
        boolean isSuccessful = WordCount.main(arg, conf);

        Map<String, Long> wordCount = new HashMap<String, Long>();

        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(new Path("/tmp/WordCountTest/WikiJavaPageResult/"));

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
        assertEquals("Key = for, Value = 2", wordCount.get("for"), "2");
        assertEquals("Key = developed, Value = 3", wordCount.get("developed"), "3");
        assertEquals("Key = is, Value = 3", wordCount.get("is"), "3");
        assertEquals("Key = as, Value = 6", wordCount.get("as"), "6");
        assertEquals("Key = the, Value = 5", wordCount.get("the"), "5");
        assertEquals("Key = in, Value = 4", wordCount.get("in"), "4");
        assertEquals("Key = java, Value = 10", wordCount.get("java"), "10");
    }

    @Test
    public void testWordCountWithWikiPythonPageDataSet() throws Exception {
        String[] arg = new String[]{"/tmp/WordCountTest/WikiPythonPage.txt", "/tmp/WordCountTest/WikiPythonPageResult/"};
        boolean isSuccessful = WordCount.main(arg, conf);

        assertTrue(isSuccessful);
    }

    @Test
    public void testWordCountWithWikiScalaPageDataSet() throws Exception {
        String[] arg = new String[]{"/tmp/WordCountTest/WikiScalaPage.txt", "/tmp/WordCountTest/WikiScalaPageResult/"};
        boolean isSuccessful = WordCount.main(arg, conf);

        assertTrue(isSuccessful);
    }
}
