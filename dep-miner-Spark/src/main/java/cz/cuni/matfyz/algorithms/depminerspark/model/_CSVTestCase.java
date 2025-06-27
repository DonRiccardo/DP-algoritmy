/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package cz.cuni.matfyz.algorithms.depminerspark.model;

import cz.cuni.matfyz.algorithms.depminerspark.model._FunctionalDependency;
import com.google.common.collect.ImmutableList;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.BitSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

/**
 *
 * @author pavel.koupil
 */
public class _CSVTestCase implements Serializable{

    private static String filePath = "";
    private static String outputFile = "";
    private static String defaultFileName = "dbtesmaData.c100000.r10";
    private static boolean defaultHasHeader = false;
    //private static BufferedWriter bw;
    private BufferedReader br;
    private Dataset<Row> df;
    private JavaRDD rddData;
    private boolean hasHeader;
    private String fileName;
    //private String nextLine;
    private int numberOfColumns;
    private ImmutableList<String> names;
    private String delimiter;
    
    
    /*
    public _CSVTestCase() throws IOException {

        this(_CSVTestCase.defaultFileName, _CSVTestCase.defaultHasHeader);
    }
    */

    public _CSVTestCase(String fileName, boolean hasHeader, SparkSession spark) throws IOException {

        this.fileName = fileName;
        
        Path p = Paths.get(fileName);        
        this.outputFile = "../output-FDs/"+p.getFileName().toString()+"-FDs-"+spark.sparkContext().appName();
        
        this.hasHeader = hasHeader;

        setDelimiter();
        
        df = spark.read().option("header", hasHeader).option("delimiter", this.delimiter).csv(fileName);        

        this.calcNumbers();
        this.getNames();

        this.rddData = df.rdd().zipWithIndex().toJavaRDD();
        _CSVTestCase.createOutputFile();
    }
    
    private void setDelimiter() throws IOException{
        
        this.br = new BufferedReader(new FileReader(new File(_CSVTestCase.filePath + fileName)));
        String nextLine = this.br.readLine();

        if (nextLine.split(",").length > nextLine.split(";").length) {
            this.delimiter = ",";
        } else {
            this.delimiter = ";";
        }   
        
        br.close();
        br = null;
        nextLine = null;
    }

    public static List<String> getAllFileNames() {

        File[] fa = new File(_CSVTestCase.filePath).listFiles();

        List<String> result = new LinkedList<>();
        for (File f : fa) {

            if (f.getName().contains(".csv")) {
                result.add(f.getName());
            }

        }

        return result;

    }

    /*
    public static void writeToResultFile(String s) throws IOException {

        bw.write(s);
        bw.newLine();
        bw.flush();
    }
    
    public static void init() throws IOException {

        _CSVTestCase.bw = new BufferedWriter(new FileWriter("Result" + System.currentTimeMillis() + ".csv"));
        bw.write("file;time;mem");
        bw.newLine();
        bw.flush();
    }

    public void close() throws IOException {

        _CSVTestCase.bw.close();
    }
    */
    private void getNames() throws IOException {

        ImmutableList.Builder<String> builder = new ImmutableList.Builder<>();

        if (this.hasHeader) {

            for (String s : this.df.columns()) {
                builder.add(s);
            }

        } else {

            for (int i = 0; i < this.numberOfColumns; i++) {

                builder.add(this.fileName + ":" + i);
            }
        }
        this.names = builder.build();

    }

    private void calcNumbers() {

        this.numberOfColumns = this.df.columns().length;
    }
    
    public JavaRDD<Tuple2<Row, Long>> getData(){
        
        return this.rddData;
    }

    public ImmutableList<String> columnNames() {

        return this.names;
    }

    public int numberOfColumns() {

        return this.numberOfColumns;
    }

    public String relationName() {

        return this.fileName;
    }

    public _CSVTestCase generateNewCopy() throws Exception {

        return this;
    }
    /*
    public void receiveResult(_FunctionalDependency fd) {

        // System.out.println(fd.getDeterminant() + "-->" + fd.getDependant());
    }

    public Boolean acceptedResult(_FunctionalDependency result) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }
    */
    
    private static void createOutputFile(){
        try {
            File myObj = new File(outputFile);
            if (myObj.createNewFile()) {
              System.out.println("Output file created: " + myObj.getName());
            } else {
                System.out.println("Output file already exists. Deleting file data.");              
                FileWriter myWriter = new FileWriter(outputFile, false);
                myWriter.write("");
                myWriter.close();   
            }
        } catch (IOException e) {
            System.out.println("An error occurred while creating outputFile.");
            
        }
        
    }
    
    public void printResultFile(HashSet<Tuple2<BitSet, Integer>> resultFDs){
        try {
            System.out.println("WRITING: resultFDs size: "+resultFDs.size());
            FileWriter myWriter = new FileWriter(outputFile, true);
            for (Tuple2<BitSet, Integer> fd : resultFDs){
                myWriter.write(fd._1+" -> "+fd._2 + System.getProperty("line.separator"));
                
            }
            myWriter.close();
            System.out.println("Successfully wrote to the output file.");
        } catch (IOException e) {
            System.out.println("An error occurred while printing results.");
            
        }
    }
    
    public void addResultToFile(_FunctionalDependencyGroup fdg){
        try {
            FileWriter myWriter = new FileWriter(outputFile, true);
            
            myWriter.write(fdg.toString() + System.getProperty("line.separator"));
            System.out.println(fdg.toString());
            
            myWriter.close();
            System.out.println("Successfully wrote to the output file.");
        } catch (IOException e) {
            System.out.println("An error occurred while printing results.");
            
        }
    }
    
    public void printResultFile(List<_FunctionalDependencyGroup> fdgList){
        try {
            System.out.println("WRITING: resultFDs size: "+fdgList.size());
            FileWriter myWriter = new FileWriter(outputFile, true);
            for (_FunctionalDependencyGroup fd : fdgList){
                myWriter.write(fd.toString() + System.getProperty("line.separator"));
                
            }
            myWriter.close();
            System.out.println("Successfully wrote to the output file.");
        } catch (IOException e) {
            System.out.println("An error occurred while printing results.");
            
        }
    }
}
