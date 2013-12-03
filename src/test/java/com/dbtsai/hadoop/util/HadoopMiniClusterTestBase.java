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

public class HadoopMiniClusterTestBase {

    private static MiniCluster miniCluster;

    @BeforeClass
    public static synchronized void oneTimeSetUp() {
        // one-time initialization code
        System.out.println("@BeforeClass - Setting Up Hadoop Mini Cluster");
        if (miniCluster == null) {
            miniCluster = new MiniCluster(1, 1);
            miniCluster.start();
        }
    }

    @AfterClass
    public static synchronized void oneTimeTearDown() {
        // one-time cleanup code
        System.out.println("@AfterClass - Tearing Down Hadoop Mini Cluster");
        if (miniCluster != null) {
            miniCluster.shutdown();
        }
    }
}
