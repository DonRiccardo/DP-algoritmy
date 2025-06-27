/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package cz.cuni.mff.algorithms.fastfds_spark;

import cz.cuni.mff.algorithms.fastfds_spark.model._CSVTestCase;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author Richard
 */
public class FastFDsSpark {
    
    public static String FILENAME;
    public static boolean hasHeader;
        
    private static SparkConf conf = new SparkConf();
    private static JavaSparkContext context = null;
    private static SparkSession spark = null;

    
     public static void main(String[] args) {
     
//        FILENAME = "../datasets/imdb-movies.csv"; hasHeader = true;
        FILENAME = "../datasets/test-example.csv"; hasHeader = true;
//        FILENAME = "../datasets/breast.csv"; hasHeader = true;
//        FILENAME = "../datasets/breastx16.csv"; hasHeader = true;
//        FILENAME = "../datasets/breastx64.csv"; hasHeader = true;
//        FILENAME = "../datasets/abalone.csv"; hasHeader = true;
//        FILENAME = "../datasets/breast-newx79.csv"; hasHeader = true;
        
        try {
                    
            // Application name to show on the cluster UI
            conf.setAppName("FastFDs-Spark");
            // cluster URL (spark://ip_address:7077) or string "local" to run in local mode
            conf.setMaster("local");

            // Context tells Spark how to access a cluster
            context = new JavaSparkContext(conf);

            spark = SparkSession.builder().appName("FastFDs-Spark").getOrCreate();
            
//			int numberOfThreads = 1;
            _CSVTestCase input = new _CSVTestCase(FILENAME, hasHeader, spark);
            //System.out.println(input.getData().collect());
            //System.out.println("HEADER: "+input.columnNames());

            long time = System.currentTimeMillis();
//			System.out.println("START: " + time);

            FastFDsSparkAlgorithm main = new FastFDsSparkAlgorithm(input, context);
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

        } 
        catch (Exception ex) {
                Logger.getLogger(FastFDsSpark.class.getName()).log(Level.SEVERE, null, ex);
        }
        
     }
}
