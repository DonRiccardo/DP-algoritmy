/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package cz.cuni.matfyz.algorithms.depminerspark;

import cz.cuni.matfyz.algorithms.depminerspark.model._CSVTestCase;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author pavel.koupil
 */
public class MainApp {

	public static String FILENAME;
        
        private static SparkConf conf = new SparkConf();
        private static JavaSparkContext context = null;
        private static SparkSession spark = null;

	public static void main(String... args) {
            boolean hasHeader;

                FILENAME = "../datasets/test-example.csv";  hasHeader = true;
//		FILENAME = "../datasets/abalone.csv";   hasHeader = false;
//		FILENAME = "../datasets/heart-2020-50t.csv";  hasHeader = true;
//		FILENAME = "../datasets/imdb-movies.csv"; hasHeader = true;
//		FILENAME = "adult.csv";		// DO NOT RUN - TOO LONG FILE
//		FILENAME = "balance-scale.csv";
//		FILENAME = "../datasets/breast.csv"; hasHeader = true;  
//		FILENAME = "breastx2.csv"; hasHeader = true;       
//		FILENAME = "../datasets/breastx16.csv"; hasHeader = true;          
//		FILENAME = "../datasets/breastx64.csv"; hasHeader = true;
//                FILENAME = "../datasets/breast-newx79.csv"; hasHeader = true;
//		FILENAME = "breast_proj.csv";
//		FILENAME = "../datasets/car10t.csv"; hasHeader = true;
//		FILENAME = "bridges.csv";
//		FILENAME = "armstrong.csv";
//		FILENAME = "echocardiogram.csv";
//		FILENAME = "flight_1k.csv";	// DO NOT RUN - TOO MANY FDs
//		FILENAME = "hepatitis.csv";	// DNR
//		FILENAME = "horse.csv";
//		FILENAME = "chess.csv";	// DO NOT RUN - TOO LONG FILE
//		FILENAME = "iris.csv";
//		FILENAME = "../datasets/letter.csv"; hasHeader = false;	// DO NOT RUN - TOO LONG FILE
//		FILENAME = "ncvoter_1001r_19c.csv";
//		FILENAME = "../datasets/nursery.csv"; hasHeader = false;	// DO NOT RUN - TOO LONG FILE
//		FILENAME = "plista_1k.csv";	// DO NOT RUN - TOO MANY FDs
//		FILENAME = "title10.csv";	// TODO: Tohle je dobrý running example, protože nad více daty platí méně funkčních závislostí - jasně řekneme, co je coincidental a budeme upravovat
//		FILENAME = "title5k.csv";	// TODO: Tohle je dobrý running example, protože nad více daty platí méně funkčních závislostí - jasně řekneme, co je coincidental a budeme upravovat
//		FILENAME = "title10k.csv";	// TODO: Tohle je dobrý running example, protože nad více daty platí méně funkčních závislostí - jasně řekneme, co je coincidental a budeme upravovat
//		FILENAME = "titanic.csv";
//		FILENAME = "armstrong.csv";


		try {
                    
                        // Application name to show on the cluster UI
                        conf.setAppName("Dep-Miner-Spark");
                        // cluster URL (spark://ip_address:7077) or string "local" to run in local mode
                        conf.setMaster("local");

                        // Context tells Spark how to access a cluster
                        context = new JavaSparkContext(conf);

                        spark = SparkSession.builder().appName("Dep-Miner-Spark").getOrCreate();
        
//			int numberOfThreads = 1;
			_CSVTestCase input = new _CSVTestCase(FILENAME, hasHeader, spark);
                        //System.out.println(input.getData().collect());
                        //System.out.println("HEADER: "+input.columnNames());

			long time = System.currentTimeMillis();
//			System.out.println("START: " + time);

			DepMinerSpark main = new DepMinerSpark(/*numberOfThreads,*/input);
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
			Logger.getLogger(MainApp.class.getName()).log(Level.SEVERE, null, ex);
		}

	}

}
