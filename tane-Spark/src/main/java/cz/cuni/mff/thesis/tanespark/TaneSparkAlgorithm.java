/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package cz.cuni.mff.thesis.tanespark;

import cz.cuni.mff.thesis.tanespark.model._CSVTestCase;
import cz.cuni.mff.thesis.tanespark.model._StrippedPartitionSpark;
import cz.cuni.mff.thesis.tanespark.service._StrippedPartitionGenerator;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectBigArrayBigList;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;


/**
 *
 * @author Richard
 */
public class TaneSparkAlgorithm implements Serializable{
    
    private final _CSVTestCase input;
    private static JavaSparkContext context;
    
    public TaneSparkAlgorithm(_CSVTestCase input, JavaSparkContext context){
        this.input = input;
        this.context = context;
    }
    
    
    private static HashSet<Tuple2<BitSet, Integer>> resultFDs = new HashSet<>();
    
    //private String tableName;
    private int numberAttributes;
    private long numberTuples;
        
    private JavaPairRDD<Integer, _StrippedPartitionSpark> plis = null;
    private Map<BitSet, CombinationHelperSpark> level0 = null;
    private JavaPairRDD<BitSet, CombinationHelperSpark> level1 = null;
    private JavaPairRDD<BitSet, List<BitSet>> prefix_blocks = null;
    
    
    public void execute() throws Exception{
        
        loadData();
        
        // Initialize Level 0
        CombinationHelperSpark chLevel0 = new CombinationHelperSpark();
        BitSet rhsCandidatesLevel0 = new BitSet();
        rhsCandidatesLevel0.set(0, numberAttributes);
        chLevel0.setRhsCandidates(rhsCandidatesLevel0);
        _StrippedPartitionSpark spLevel0 = new _StrippedPartitionSpark(numberTuples);
        chLevel0.setPartition(spLevel0);
        spLevel0 = null;
        level0 = new HashMap<>();
        level0.put(new BitSet(), chLevel0);
        
        // Initialize Level 1        
        ArrayList<Tuple2<BitSet, CombinationHelperSpark>> listLVL1 = new ArrayList<>();
        
        for (int i = 0; i < numberAttributes; i++) {
            BitSet combinationLevel1 = new BitSet();
            combinationLevel1.set(i);

            CombinationHelperSpark chLevel1 = new CombinationHelperSpark();
            BitSet rhsCandidatesLevel1 = new BitSet();
            rhsCandidatesLevel1.set(0, numberAttributes);
            chLevel1.setRhsCandidates(rhsCandidatesLevel1);
            
            _StrippedPartitionSpark spLevel1;
            List<_StrippedPartitionSpark> lsp = plis.lookup(i);
            if (lsp.size() == 1){
                spLevel1 = lsp.get(0);
            }
            else{
                spLevel1 = new _StrippedPartitionSpark(numberTuples);
            }
             
            chLevel1.setPartition(spLevel1);
            listLVL1.add(new Tuple2<>(combinationLevel1, chLevel1));            
        }
        
        
        level1 = context.parallelizePairs(listLVL1);
        listLVL1 = null;
        
        // while loop (main part of TANE)
        /*
        Map<BitSet, CombinationHelperSpark> lvl = level1.collectAsMap();
        for (BitSet b:lvl.keySet()){
            System.out.println(b.toString() +" "+lvl.get(b).isValid()+" RHSC: "+lvl.get(b).getRhsCandidates());
        }
        */
        int l = 0;
        while (level1.count()>0 && l < numberAttributes) {
            //System.out.println("START "+l+". loop!");
            //System.out.println("# ELEMENTS in LEVEL1, in "+l+". loop START: "+level1.keys().count());
            // compute dependencies for a level
            System.out.println("\n START LVL: "+l +"\n");
            computeDependencies();
            //System.out.println("# ELEMENTS in LEVEL1, in "+l+". loop COMDEP: "+level1.keys().count());
            // prune the search space
            prune();
            //System.out.println("# ELEMENTS in LEVEL1, in "+l+". loop PRUNE: "+level1.keys().count());
            
            /*
            Map<BitSet, CombinationHelperSpark> lvl1 = level1.collectAsMap();
            for (BitSet b:lvl1.keySet()){
                System.out.println(b.toString() +" "+lvl1.get(b).isValid()+" RHSC: "+lvl1.get(b).getRhsCandidates());
            }           
            */
            // compute the combinations for the next level
            generateNextLevel();     
             
            
            //System.out.println("# ELEMENTS in LEVEL1, in "+l+". loop GENLVL: "+level1.keys().count());           
            //System.out.println("ENDING "+l+". loop.");
            l++;
        }
        
        
        printFDsIntoFile();
    }
    
    // vytvorenie Stripped Partitions
    private void loadData() throws Exception{
       
        this.numberAttributes = input.numberOfColumns();
        this.numberTuples = input.numberOfRows();
       
        _StrippedPartitionGenerator spGen = new _StrippedPartitionGenerator();
        plis = spGen.execute(this.input);
        System.out.println("SP DONE...");
    }
    
    private void computeDependencies() {
        // inicializuje sa Cplus rovno v generovani noveho levlu
        //initializeCplusForLevel();
        
        level1 = level1
                .mapToPair(tuple -> {
                    if(!tuple._2.isValid()){
                        return tuple;
                    }
                    
                    BitSet intersection = (BitSet) tuple._1.clone();
                    intersection.and(tuple._2.getRhsCandidates());
                    
                    BitSet Xclone = (BitSet) tuple._1.clone();
                                        
                    for (int A = intersection.nextSetBit(0); A >= 0; A = intersection.nextSetBit(A + 1)){
                        Xclone.clear(A);
                        //System.out.println("Xclone: "+Xclone+" LVL0 = "+level0.get(Xclone));
                        if(!level0.get(Xclone).isValid()){
                            Xclone.set(A);
                            continue;
                        }
                        
                        _StrippedPartitionSpark spX = tuple._2.getPartition();
                        _StrippedPartitionSpark spXwithoutA = level0.get(Xclone).getPartition();
                        
                        if(spX.getError() == spXwithoutA.getError()){
                            
                            processFunctionalDependency((BitSet)Xclone.clone(), A, "Computation");
                           
                            // remove A from C_plus(X)    
                            BitSet newRhsCandidates = (BitSet) tuple._2.getRhsCandidates().clone();
                            newRhsCandidates.clear(A);

                            // remove all B in R\X from C_plus(X)
                            BitSet RwithoutX = new BitSet();
                            // set to R
                            RwithoutX.set(0, numberAttributes);
                            // remove X
                            RwithoutX.andNot(tuple._1);

                            for (int i = RwithoutX.nextSetBit(0); i >= 0; i = RwithoutX.nextSetBit(i + 1)) {
                                newRhsCandidates.clear(i);
                            }
                            
                            tuple._2.setRhsCandidates(newRhsCandidates);
                            
                        }
                        
                        Xclone.set(A);
                    }
                    
                    return tuple;
                });
        
        /*
        level1 = level1
            .filter(x -> x._2.isValid())
            .flatMap(tuple -> {
                // create TUPLE<<tuple>, spXwithoutA, spX, A> to chceck if FD X\A->A is valid
                List<Tuple4<Tuple2<BitSet, CombinationHelperSpark>, _StrippedPartitionSpark, _StrippedPartitionSpark, Integer>> depXwithoutAandA = new ArrayList<>();
                
                BitSet C_plus = tuple._2.getRhsCandidates();
                BitSet intersection = (BitSet) tuple._1.clone();
                intersection.and(C_plus);
                
                
                // clone of X for usage in the following loop
                BitSet Xclone = (BitSet) tuple._1.clone();
                
                for (int A = intersection.nextSetBit(0); A >= 0; A = intersection.nextSetBit(A + 1)) {
                    Xclone.clear(A);
                    //System.out.println("");
                    // check if X\A -> A is valid
                    
                    if (level0.get(Xclone).isValid()){
                        _StrippedPartitionSpark spXwithoutA = level0.get(Xclone).getPartition();
                        //System.out.println("spXwithouA: "+level0.get(Xclone));
                        _StrippedPartitionSpark spX = tuple._2.getPartition();
                        //System.out.println("spX: "+tuple._2);
                        //System.out.println("Computing: X="+tuple._1+" O: "+ spX + "   X\\A="+Xclone+" O: "+ spXwithoutA);

                        //System.out.println("Computing: X="+tuple._1+" E: "+ spX.getError() + "   X\\A="+Xclone+" E: "+ spXwithoutA.getError());
                        depXwithoutAandA.add(new Tuple4<>(tuple, spXwithoutA, spX, A));
                    }
                    
                    Xclone.set(A);
                }
                //System.out.println("depXwithoutAandA SIZE: "+depXwithoutAandA.size());
                return depXwithoutAandA.iterator();
            })
            // filter only valid dependencies 
            // TUPLE<<tuple>, spXwithoutA, spX, A>
            .filter(x -> x._2().getError() == x._3().getError())
            .mapToPair(tuple -> {
                BitSet XwithoutA = (BitSet) tuple._1()._1.clone();
                XwithoutA.clear(tuple._4());
                processFunctionalDependency(XwithoutA, tuple._4(), "Computation");
               // System.out.println("Compute BEFORE: "+tuple._1()._1+" RHSC: "+tuple._1()._2.getRhsCandidates());
                // remove A from C_plus(X)    
                BitSet newRhsCandidates = (BitSet) tuple._1()._2.getRhsCandidates().clone();
                newRhsCandidates.clear(tuple._4());

                // remove all B in R\X from C_plus(X)
                BitSet RwithoutX = new BitSet();
                // set to R
                RwithoutX.set(0, numberAttributes);
                // remove X
                RwithoutX.andNot(tuple._1()._1);

                for (int i = RwithoutX.nextSetBit(0); i >= 0; i = RwithoutX.nextSetBit(i + 1)) {
                    newRhsCandidates.clear(i);
                }
                //System.out.println("Compute AFTER:  "+tuple._1()._1+" RHSC: "+newRhsCandidates);
                tuple._1()._2.setRhsCandidates(newRhsCandidates);
                return new Tuple2<>(tuple._1()._1, tuple._1()._2);
            })
            .reduceByKey((ch1, ch2) -> {
                BitSet combinedRhs = (BitSet) ch1.getRhsCandidates().clone();
                combinedRhs.and(ch2.getRhsCandidates());
                
                ch1.setRhsCandidates(combinedRhs);
                return ch1;
            })
            .rightOuterJoin(level1)
            .mapValues(tuple -> {
                if(tuple._1.isPresent()){
                    //System.out.println("CHOOSING "+ tuple._1.orNull().getRhsCandidates() +" or "+tuple._2.getRhsCandidates());
                }
                else{
                  //  System.out.println("CHOOSING -- or "+tuple._2.getRhsCandidates());
                }
                return tuple._1.orElse(tuple._2);})
            .mapToPair(tuple ->{
               // System.out.println("New level1: "+tuple._1+" RHSC = "+tuple._2.getRhsCandidates());
                return tuple;
            })
            ;
        */
        
        
    }
    
    /*
    private void initializeCplusForLevel(){
    
        // presunute na koniec -> Generate Next Level
    
        level1 = level1
            .flatMapToPair(tuple -> {
                List<Tuple2<Tuple2<BitSet, CombinationHelperSpark>, BitSet>> rhsCwithoutA = new ArrayList<>();

                BitSet Xclone = (BitSet) tuple._1.clone();
                for (int A = tuple._1.nextSetBit(0); A >= 0; A = tuple._1.nextSetBit(A + 1)) {
                    Xclone.clear(A);
                    BitSet CxwithoutA = level0.get(Xclone).getRhsCandidates();
                    rhsCwithoutA.add(new Tuple2<>(tuple, CxwithoutA));
                    System.out.println("FOR "+tuple._1+" found "+Xclone+" with RHSC-> "+CxwithoutA);
                    Xclone.set(A);
                }

                if (rhsCwithoutA.isEmpty()){
                    rhsCwithoutA.add(new Tuple2<>(tuple, new BitSet()));
                }

                return rhsCwithoutA.iterator();
            })
            .reduceByKey((x, y) -> {
                //System.out.println("X: "+x+" & Y: "+y);
                BitSet newRhs = (BitSet) x.clone();
                newRhs.and(y);
                //System.out.println("AND: "+newRhs);
                return newRhs;
            })
            .mapToPair(c -> {
                System.out.println("Setting RHSC "+c._1._1+"->"+c._2);
                c._1._2.setRhsCandidates(c._2);
                return c._1;
            });
        
    }
    */
    
    private void prune(){
        
        Map<BitSet, CombinationHelperSpark> level1asMap = level1.collectAsMap();
        
        level1 = level1
                .mapToPair(tuple -> {
                      
                    if(!tuple._2.isValid() || tuple._2.getPartition().getError()!=0){
                        
                        return tuple;
                    }
                    
                    BitSet rhsXwithoutX = (BitSet) tuple._2.getRhsCandidates().clone();
                    rhsXwithoutX.andNot(tuple._1);
                    BitSet xUnionAWithoutB = (BitSet) tuple._1.clone();
                    
                    for (int A = rhsXwithoutX.nextSetBit(0); A >= 0; A = rhsXwithoutX.nextSetBit(A + 1)){
                                                
                        BitSet intersect = new BitSet();
                        intersect.set(0, numberAttributes);
                        
                        xUnionAWithoutB.set(A);

                        for (int B = tuple._1.nextSetBit(0); B >= 0; B = tuple._1.nextSetBit(B + 1)){
                            xUnionAWithoutB.clear(B);
                            intersect.and(level1asMap.get(xUnionAWithoutB).getRhsCandidates());
                            xUnionAWithoutB.set(B);
                        }
                        
                        if(intersect.get(A)){

                            BitSet lhs = (BitSet) tuple._1.clone();
                            processFunctionalDependency(lhs, A, "Prune");
                            
                            BitSet newRhs = (BitSet) tuple._2.getRhsCandidates().clone();
                            newRhs.clear(A);
                            tuple._2.setRhsCandidates(newRhs);
                            tuple._2.setInvalid();
                        }

                        xUnionAWithoutB.clear(A);
                    }
                    
                    
                    return tuple;
                })
                .reduceByKey((x,y) -> x);
             
        //level1.count();
        
        
        /*
        level1 = level1
            .filter(x -> !x._2.getRhsCandidates().isEmpty())
            .filter(x -> x._2.isValid() && x._2.getPartition().getError() == 0)
            .flatMapToPair(tuple -> {
                //System.out.println("Prune tuple: "+tuple._1.toString()+" = "+tuple._2.getRhsCandidates().toString()+", "+tuple._2.getPartition().getError());
                BitSet x = tuple._1;
                List<Tuple2<BitSet, Tuple3<BitSet, CombinationHelperSpark, Integer>>> entities = new ArrayList<>();
                
                BitSet rhsXwithoutX = (BitSet) tuple._2.getRhsCandidates().clone();
                rhsXwithoutX.andNot(x);
                for (int a = rhsXwithoutX.nextSetBit(0); a >= 0; a = rhsXwithoutX.nextSetBit(a + 1)) {
                    BitSet intersect = new BitSet();
                    intersect.set(0, numberAttributes);

                    BitSet xUnionAWithoutB = (BitSet) x.clone();
                    xUnionAWithoutB.set(a);
                    for (int b = x.nextSetBit(0); b >= 0; b = x.nextSetBit(b + 1)) {
                        xUnionAWithoutB.clear(b);
                        entities.add(new Tuple2<>((BitSet)xUnionAWithoutB.clone(), new Tuple3<>(tuple._1, tuple._2, a)));
                        xUnionAWithoutB.set(b);
                    }
                }
                
                return entities.iterator();
            })
            .leftOuterJoin(level1)
            .mapToPair(tuple -> {
                // generate new TUPLE<<BitSet of X from level1, A>, <ComHelp of X, RHS candidates from ComHelp of XunionAwithoutB>>
                
                BitSet bs = null;
                if (tuple._2._2.isPresent()){
                    bs = tuple._2._2.get().getRhsCandidates();
                }
                return new Tuple2<>(new Tuple2<>(tuple._2._1._1(), tuple._2._1._3()), new Tuple2<>(tuple._2._1._2(), bs));
                    
            })
            .reduceByKey((x, y) -> {
                BitSet newBitSet = (BitSet) x._2.clone();
                newBitSet.and(y._2);
                
                return new Tuple2<>(x._1, newBitSet);
            })
            .filter(x -> x._2._2.get(x._1._2))
            .mapToPair(x -> {
                BitSet lhs = (BitSet) x._1._1.clone();
                processFunctionalDependency(lhs, x._1._2, "Prune");
                x._2._1.getRhsCandidates().clear(x._1._2);
                x._2._1.setInvalid();
                
                return new Tuple2<>(x._1._1, x._2._1);
            })
            .rightOuterJoin(level1)
            .mapValues(tuple -> tuple._1.orElse(tuple._2));
        
        */
    }

    
    /*
    private StrippedPartitionSpark multiply(StrippedPartitionSpark sp1, StrippedPartitionSpark sp2){
        // KEY: Id of row in dataset, VALUE: <index of PLIS1 containing Id, index of PLIS containing Id, Id>
        Map<Long, Tuple2<Tuple2<Long, Long>, Long>> partitions = new HashMap<>();
        ObjectBigArrayBigList<LongBigArrayBigList> partition1 = sp1.getStrippedPartition();
        ObjectBigArrayBigList<LongBigArrayBigList> partition2 = sp2.getStrippedPartition();
        
        for (long i = 0; i < partition1.size64() ; i++) {
            LongBigArrayBigList actualPartition = partition1.get(i);
            for (long Id : actualPartition) {
                partitions.put(Id, new Tuple2<>(new Tuple2<>(i, -1L), Id));
            }
        }
        
        for (long i = 0; i < partition2.size64() ; i++) {
            LongBigArrayBigList actualPartition = partition2.get(i);
            for (long Id : actualPartition) {
                if (partitions.containsKey(Id)){
                    Tuple2<Tuple2<Long, Long>, Long> valueOld = partitions.get(Id);
                    partitions.put(Id, new Tuple2<>(new Tuple2<>(valueOld._1._1(), i), Id));
                }
            }
        }
                
        ArrayList<Tuple2<Tuple2<Long, Long>, Long>> partitionValues = new ArrayList<>(partitions.values());
        System.out.println("NUM: "+partitionValues.size());
        for (int i = 0; i < partitionValues.size(); i++) {
            System.out.println(partitionValues.get(i).toString());
        }
        
        //JavaRDD partitionRDD = context.parallelize(partitionValues);
        //System.out.println("NUM RDD: "+partitionRDD.count());
        //JavaPairRDD<Tuple2<Long, Long>, Long> partitionsTrio = JavaPairRDD.fromJavaRDD(partitionRDD);
        
        //JavaPairRDD<Tuple2<Long, Long>, Long> partitionsTrio = context.parallelizePairs(partitionValues);
        JavaPairRDD<Tuple2<Long, Long>, Long> partitionsTrio = context.parallelizePairs(new ArrayList<>());

        partitionValues = null;
        System.out.println("NUM PairRDD: "+partitionsTrio.collect().size());
        
        
        JavaRDD<StrippedPartitionSpark> multipliedPartition = partitionsTrio
                .filter(x -> x._1._2 > -1L)
                .groupByKey()
                .mapToPair(tuple -> {
                    System.out.println("OKAY");
                    LongBigArrayBigList list = new LongBigArrayBigList();
                    tuple._2.forEach(list::add);
                    ObjectBigArrayBigList<LongBigArrayBigList> partition = new ObjectBigArrayBigList<>();
                    partition.add(list);
                    return new Tuple2<>(true, new Tuple2<>(partition, list.size64()));
                })
                .reduceByKey((x, y) -> {
                    x._1.addAll(y._1);
                    long elemCount = x._2 + y._2;
                    return new Tuple2<>(x._1, elemCount);
                })
                .map(x -> {
                    StrippedPartitionSpark newSP = new StrippedPartitionSpark(x._2._1, x._2._2);
                    return newSP;
                });
        System.out.println("MULTIPLY: "+multipliedPartition.first().getError());
        return (multipliedPartition.count() > 0) ? multipliedPartition.first() : new StrippedPartitionSpark(numberTuples);
        
    }
    */
    
    public _StrippedPartitionSpark multiply(_StrippedPartitionSpark pt1, _StrippedPartitionSpark pt2) {
        LongBigArrayBigList tTable;
        tTable = new LongBigArrayBigList(numberTuples);
        for (long i = 0; i < numberTuples; i++) {
            tTable.add(-1);
        }
        List<LongList> result = new ArrayList<>();
        List<LongList> pt1List = pt1.getStrippedPartition();
        List<LongList> pt2List = pt2.getStrippedPartition();
        List<LongList> partition = new ArrayList<>();
        long noOfElements = 0;
        // iterate over first stripped partition and fill tTable.
        for (int i = 0; i < pt1List.size(); i++) {
            for (long tId : pt1List.get(i)) {
                tTable.set(tId, i);
            }
            partition.add(new LongArrayList());
        }
        // iterate over second stripped partition.
        for (int i = 0; i < pt2List.size(); i++) {
            for (long t_id : pt2List.get(i)) {
                // tuple is also in an equivalence class of pt1
                if (tTable.get(t_id) != -1) {
                    partition.get(tTable.get(t_id).intValue()).add(t_id);
                }
            }
            for (long tId : pt2List.get(i)) {
                // if condition not in the paper;
                if (tTable.get(tId) != -1) {
                    if (partition.get(tTable.get(tId).intValue()).size() > 1) {
                        LongList eqClass = partition.get(tTable.get(tId).intValue());
                        result.add(eqClass);
                        noOfElements += eqClass.size();
                    }
                    partition.set(tTable.get(tId).intValue(), new LongArrayList());
                }
            }
        }
        // cleanup tTable to reuse it in the next multiplication.
        // Zbytocne, kedze tabulka sa vytvara nova v kazdom zavolani multiply -> zmenit na jednu centralnu tabulku s vymazavanim?
        /*
        for (long i = 0; i < pt1List.size64(); i++) {
            for (long tId : pt1List.get(i)) {
                tTable.set(tId, -1);
            }
        }
        */
        /*
        System.out.println("NEW Multiplied SP = NUMelements: "+noOfElements);
            for (int i=0; i< result.size64(); i++){
                System.out.print("{");
                for (Object o : result.get(i)) {
                    System.out.print(o+", ");
                }
                System.out.print("}, ");
            }
            System.out.println();
        
        */
        return new _StrippedPartitionSpark(result, noOfElements);
    }
    
    
    
    
    private void generateNextLevel(){
        // LVL1 collectujem aj v PRUNE, aby som s tym mohol pracovat. Pouziva sa na overenie validity
        // narocne, ale nie je mozne pracovat s vnorenymi RDD
        level0 = level1.collectAsMap();
        /*
        for(BitSet b : level0.keySet()){
            System.out.println("KEY: "+b+"VALUE: "+level0.get(b).toString());
        }
        */
        
        //System.out.println("po collectAsMap -> LVL1 "+level1.count());
        
        level1 = null;
        buildPrefixBlocks();
        //System.out.println("GENLVL after prefix blocks: "+prefix_blocks.collect().size()+" entities");
        //System.out.println("GENERATING NEW LEVEL1...");
        level1 = prefix_blocks
                .filter(x -> x._2.size() >= 2)
                .flatMapToPair(tuple -> {
                    List<Tuple2<BitSet, BitSet>> combinations = getListCombinations(tuple._2);
                    return combinations.iterator();
                })
                .map(tuple -> {
                    BitSet X = (BitSet) tuple._1.clone();
                    X.or(tuple._2);
                    return new Tuple3<>(tuple._1, tuple._2, X);
                })
                .filter(x -> checkSubsets(x._3()))
                .mapToPair(tuple -> {
                   // System.out.println("TUPLE: "+tuple._1()+" & "+tuple._2()+" OR "+tuple._3());
                    
                    _StrippedPartitionSpark st = null;
                    CombinationHelperSpark ch = new CombinationHelperSpark();
                    
                    if (level0.get(tuple._1()).isValid() && level0.get(tuple._2()).isValid()) {
                        st = multiply(level0.get(tuple._1()).getPartition(), level0.get(tuple._2()).getPartition());
                        //System.out.println("VALID in level0");
                    } else {
                        ch.setInvalid();
                        //System.out.println("INVALID in level0");
                    }
                    //System.out.println("NEW LVL BitSet: "+tuple._3()+" SP: "+st);
                    BitSet rhsCandidates = new BitSet();

                    ch.setPartition(st);
                    ch.setRhsCandidates(rhsCandidates);
                    
                    return new Tuple2<>(tuple._3(), ch);
                    
                })
                
                // INITIALIZECPLUSFORLEVEL
                // skombinovanie funkcii, pretoze pred prvym spustenim levlu to uz nastavene je
                // dalsie levly sa pocitaju v generovani, tak rovno nastavit aj Cplus
                .flatMapToPair(tuple -> {
                    List<Tuple2<Tuple2<BitSet, CombinationHelperSpark>, BitSet>> rhsCwithoutA = new ArrayList<>();

                    BitSet Xclone = (BitSet) tuple._1.clone();
                    for (int A = tuple._1.nextSetBit(0); A >= 0; A = tuple._1.nextSetBit(A + 1)) {
                        Xclone.clear(A);
                        BitSet CxwithoutA = level0.get(Xclone).getRhsCandidates();
                        rhsCwithoutA.add(new Tuple2<>(tuple, CxwithoutA));
                      //  System.out.println("FOR "+tuple._1+" found "+Xclone+" with RHSC-> "+CxwithoutA);
                        Xclone.set(A);
                    }

                    if (rhsCwithoutA.isEmpty()){
                        rhsCwithoutA.add(new Tuple2<>(tuple, new BitSet()));
                    }

                    return rhsCwithoutA.iterator();
                })
                .reduceByKey((x, y) -> {
                    //System.out.println("X: "+x+" & Y: "+y);
                    BitSet newRhs = (BitSet) x.clone();
                    newRhs.and(y);
                    //System.out.println("AND: "+newRhs);
                    return newRhs;
                })
                .mapToPair(c -> {
                   // System.out.println("Setting RHSC "+c._1._1+"->"+c._2);
                    c._1._2.setRhsCandidates(c._2);
                    return c._1;
                });
        
        //System.out.println("GENERATING NEW LEVEL1...DONE");
    }
    
    private int getLastSetBitIndex(BitSet bitset) {
        int lastSetBit = 0;
        for (int A = bitset.nextSetBit(0); A >= 0; A = bitset.nextSetBit(A + 1)) {
            lastSetBit = A;
        }
        return lastSetBit;
    }
    
    /**
     * Get prefix of BitSet by copying it and removing the last Bit.
     *
     * @param bitset
     * @return A new BitSet, where the last set Bit is cleared.
     */
    private BitSet getPrefix(BitSet bitset) {
        BitSet prefix = (BitSet) bitset.clone();
        prefix.clear(getLastSetBitIndex(prefix));
        return prefix;
    }
    
    /**
     * Build the prefix blocks for a level. It is a HashMap containing the
     * prefix as a key and the corresponding attributes as  the value.
     */
    private void buildPrefixBlocks() {        
        List<Tuple2<BitSet, BitSet>> listPrefixBlocks = new ArrayList<>();
        
        for (BitSet level_iter : level0.keySet()) {
            BitSet prefix = getPrefix(level_iter);
            listPrefixBlocks.add(new Tuple2<>(prefix, level_iter));            
        }
        
        prefix_blocks = context.parallelizePairs(listPrefixBlocks)
                .groupByKey()
                .mapToPair(tuple -> {
                    List<BitSet> list = new ArrayList<>();
    
                    for (BitSet bitset : tuple._2) {
                        list.add(bitset);
                    }
                    return new Tuple2<>(tuple._1, list);
                });
    }
    
    
    /**
     * Get all combinations, which can be built out of the elements of a prefix block
     *
     * @param list: List of BitSets, which are in the same prefix block.
     * @return All combinations of the BitSets.
     */
    private List<Tuple2<BitSet, BitSet>> getListCombinations(List<BitSet> list) {
        List<Tuple2<BitSet, BitSet>> combinations = new ObjectArrayList<>();
        for (int a = 0; a < list.size(); a++) {
            for (int b = a + 1; b < list.size(); b++) {
                combinations.add(new Tuple2<>(list.get(a), list.get(b)));
                //System.out.println("COMBINATIONS: "+list.get(a)+" & "+list.get(b));
            }
        }
        return combinations;
    }
    
    /**
     * Checks whether all subsets of X (with length of X - 1) are part of the last level.
     * Only if this check return true X is added to the new level.
     *
     * @param X
     * @return
     */
    private boolean checkSubsets(BitSet X) {
        boolean xIsValid = true;

        // clone of X for usage in the following loop
        BitSet Xclone = (BitSet) X.clone();

        for (int l = X.nextSetBit(0); l >= 0; l = X.nextSetBit(l + 1)) {
            Xclone.clear(l);
            if (!level0.containsKey(Xclone)) {
                xIsValid = false;
                break;
            }
            Xclone.set(l);
        }

        return xIsValid;
    }
    

    private void processFunctionalDependency(BitSet XwithoutA, Integer A, String founder) {
        //System.out.println("FUNCTIONAL DEPENDENCY from "+founder+": "+XwithoutA.toString() +" -> "+ A);
        resultFDs.add(new Tuple2<>(XwithoutA, A));
    }

    private void printFDsIntoFile(){
        input.printResultFile(resultFDs);
    }
    
}








