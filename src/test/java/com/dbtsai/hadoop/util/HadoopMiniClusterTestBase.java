package com.dbtsai.hadoop.util;

import org.apache.hadoop.conf.Configuration;
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

    public static Configuration getConfiguration() {
        return miniCluster.configuration().get();
    }

    @BeforeClass
    public static synchronized void oneTimeSetUp() {
        // one-time initialization code
        if (miniCluster == null) {
            System.out.println("\n@BeforeClass - Setting Up Hadoop Mini Cluster\n");
            miniCluster = new MiniCluster(1, 1);
            miniCluster.start();
        }
    }

//    @AfterClass
//    public static synchronized void oneTimeTearDown() {
//        // one-time cleanup code
//        if (miniCluster != null) {
//            System.out.println("\n@AfterClass - Tearing Down Hadoop Mini Cluster\n");
//            miniCluster.shutdown();
//            miniCluster = null;
//        }
//    }
}
