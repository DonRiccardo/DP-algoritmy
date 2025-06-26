/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 */

package cz.cuni.mff.thesis.tanespark;

import cz.cuni.mff.thesis.tanespark.model._CSVTestCase;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.SparkSession;


/**
 *
 * @author Richard
 */
public class TaneSpark {
    
    
    public static String FILENAME;
    public static boolean hasHeader;
        
    private static SparkConf conf = new SparkConf();
    private static JavaSparkContext context = null;
    private static SparkSession spark = null;

    
//    private Object2ObjectOpenHashMap<BitSet, CombinationHelper> level0 = null;
//    private Object2ObjectOpenHashMap<BitSet, CombinationHelper> level1 = null;
//    private Object2ObjectOpenHashMap<BitSet, ObjectArrayList<BitSet>> prefix_blocks = null;

    public static void main(String[] args) {
        
//        FILENAME = "imdb-movies.csv"; hasHeader = true;
        FILENAME = "test-example.csv"; hasHeader = true;
//        FILENAME = "wisconsin-breast-cancer-x1.csv"; hasHeader = true;
//        FILENAME = "breastx16.csv"; hasHeader = true;
//        FILENAME = "breastx64.csv"; hasHeader = true;
//        FILENAME = "abalone.csv"; hasHeader = true;
        
        try {
                    
            // Application name to show on the cluster UI
            conf.setAppName("Tane-Spark");
            // cluster URL (spark://ip_address:7077) or string "local" to run in local mode
            conf.setMaster("local");

            // Context tells Spark how to access a cluster
            context = new JavaSparkContext(conf);

            spark = SparkSession.builder().appName("Tane-Spark").getOrCreate();
            
//			int numberOfThreads = 1;
            _CSVTestCase input = new _CSVTestCase(FILENAME, hasHeader, spark);
            //System.out.println(input.getData().collect());
            //System.out.println("HEADER: "+input.columnNames());

            long time = System.currentTimeMillis();
//			System.out.println("START: " + time);

            TaneSparkAlgorithm main = new TaneSparkAlgorithm(input, context);
            main.execute();
            time = System.currentTimeMillis() - time;
            System.out.println("Time: " + time);

//			if (FILENAME.equals("breast_proj.csv")) {
//				main.demo();
//			}
//			
//			if (FILENAME.equals("titanic.csv")) {
//				main.demo2();
//			}

        } catch (Exception ex) {
                Logger.getLogger(TaneSpark.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        
    }
    
    
}
