/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package cz.cuni.mff.algorithms.hyfd_spark;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import cz.cuni.mff.algorithms.hyfd_spark.model._CSVTestCase;

/**
 *
 * @author Richard
 */
public class HyFDSpark {
    
    public static String FILENAME;
    public static boolean hasHeader;
        
    private static SparkConf conf = new SparkConf();
    private static JavaSparkContext context = null;
    private static SparkSession spark = null;
    
    public static void main(String[] args) {
        
        FILENAME = "../datasets/imdb-movies.csv"; hasHeader = true;
//        FILENAME = "../datasets/test-example.csv"; hasHeader = true;
//        FILENAME = "../datasets/breast.csv"; hasHeader = true;
//        FILENAME = "../datasets/breastx16.csv"; hasHeader = true;l
//        FILENAME = "../datasets/breastx64.csv"; hasHeader = true;
//       FILENAME = "../datasets/breast-newx79.csv"; hasHeader = true;
//        FILENAME = "../datasets/abalone.csv"; hasHeader = false;
        
        try {
                    
            // Application name to show on the cluster UI
            conf.setAppName("HyFD-Spark");
            // cluster URL (spark://ip_address:7077) or string "local" to run in local mode
            conf.setMaster("local");

            // Context tells Spark how to access a cluster
            context = new JavaSparkContext(conf);

            spark = SparkSession.builder().appName("FDep-Spark").getOrCreate();
            
//			int numberOfThreads = 1;
            _CSVTestCase input = new _CSVTestCase(FILENAME, hasHeader, spark);
            //System.out.println(input.getData().collect());
            //System.out.println("HEADER: "+input.columnNames());

            long time = System.currentTimeMillis();
//			System.out.println("START: " + time);

            HyFDSparkAlgorithm main = new HyFDSparkAlgorithm(input, context);
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
                Logger.getLogger(HyFDSparkAlgorithm.class.getName()).log(Level.SEVERE, "Something went wrong.", ex);
        }
        
    }
}
