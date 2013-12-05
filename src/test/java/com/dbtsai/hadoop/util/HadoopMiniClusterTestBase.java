package com.dbtsai.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;

/**
 * Created with IntelliJ IDEA.
 * User: dbtsai
 * Date: 12/2/13
 * Time: 5:36 PM
 * To change this template use File | Settings | File Templates.
 */

public class HadoopMiniClusterTestBase {

    private MiniCluster miniCluster;

    public Configuration getConfiguration() {
        return miniCluster.configuration().get();
    }

    @Before
    public void setUp() {
        // one-time initialization code
        if (miniCluster == null) {
            System.out.println("\n@BeforeClass - Setting Up Hadoop Mini Cluster\n");
            miniCluster = new MiniCluster(1, 1);
            miniCluster.start();
        }
    }

    @After
    public void tearDown() {
        // one-time cleanup code
        if (miniCluster != null) {
            System.out.println("\n@AfterClass - Tearing Down Hadoop Mini Cluster\n");
            miniCluster.shutdown();
            miniCluster = null;
        }
    }
}
