package com.dbtsai.hadoop.util;

import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Created with IntelliJ IDEA.
 * User: dbtsai
 * Date: 12/2/13
 * Time: 5:36 PM
 * To change this template use File | Settings | File Templates.
 */

public class HadoopTestBase {

    private static MiniCluster miniCluster;

    @BeforeClass
    public static void oneTimeSetUp() {
        // one-time initialization code
        miniCluster = new MiniCluster(1, 1);
        miniCluster.start();
        System.out.println("@BeforeClass - Setting Up Hadoop Mini Cluster");
    }

    @AfterClass
    public static void oneTimeTearDown() {
        // one-time cleanup code
        miniCluster.shutdown();
        System.out.println("@AfterClass - Tearing Down Hadoop Mini Cluster");
    }
}
