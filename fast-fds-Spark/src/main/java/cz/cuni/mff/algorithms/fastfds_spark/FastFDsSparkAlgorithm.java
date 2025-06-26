package cz.cuni.mff.algorithms.fastfds_spark;

//import de.metanome.algorithm_integration.AlgorithmExecutionException;
//import de.metanome.algorithm_integration.input.RelationalInput;
//import de.metanome.algorithm_integration.result_receiver.FunctionalDependencyResultReceiver;
import cz.cuni.mff.algorithms.fastfds_spark.model._TupleEquivalenceClassRelation;
import cz.cuni.mff.algorithms.fastfds_spark.services._FindCoversGenerator;
import cz.cuni.mff.algorithms.fastfds_spark.model._CSVTestCase;
import cz.cuni.mff.algorithms.fastfds_spark.model._StrippedPartitionSpark;
import cz.cuni.mff.algorithms.fastfds_spark.model._DifferenceSet;
import cz.cuni.mff.algorithms.fastfds_spark.services._DifferenceSetFromAgreeSetGenerator;
import cz.cuni.mff.algorithms.fastfds_spark.services._StrippedPartitionGenerator;
//import it.unimi.dsi.fastutil.longs.AbstractLong2ObjectMap;
//import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.io.Serializable;
import java.util.BitSet;
import java.util.HashSet;

//import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class FastFDsSparkAlgorithm implements Serializable{

    private final _CSVTestCase input;
    private static JavaSparkContext context;
    private static Long2ObjectOpenHashMap<_TupleEquivalenceClassRelation> relationships = new Long2ObjectOpenHashMap<_TupleEquivalenceClassRelation>();
    private static HashSet<Tuple2<BitSet, Integer>> outputFD;
    
    public FastFDsSparkAlgorithm(_CSVTestCase input, JavaSparkContext context) {

        this.input = input;
        this.context = context;
        this.outputFD = new HashSet<>();
    }

    public void execute() /*throws AlgorithmExecutionException*/ {
                
        

        JavaPairRDD<Integer, _StrippedPartitionSpark> strippedPartitions = new _StrippedPartitionGenerator(relationships).execute(input);
        /*
        strippedPartitions.collect();
        for (long i : relationships.keySet()) {
            System.out.println(i+": "+relationships.get(i).toString());
        }
        */
        JavaRDD<_DifferenceSet> diff = new _DifferenceSetFromAgreeSetGenerator(relationships, input.numberOfColumns()).executeBottleneck(strippedPartitions);
 
        /*
        List<_DifferenceSet> diffList = diff.collect();
        System.out.println("DIIF: "+diffList.size()+" size");
        for (_DifferenceSet d : diffList){
            System.out.println(d.toString());
        }
        */

        new _FindCoversGenerator( input.columnNames(), input.relationName(), input.numberOfColumns(), this.outputFD).execute(diff);

        this.input.printResultFile(outputFD);
    }

    
}
