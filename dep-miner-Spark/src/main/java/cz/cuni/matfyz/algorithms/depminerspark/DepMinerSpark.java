/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package cz.cuni.matfyz.algorithms.depminerspark;

import cz.cuni.matfyz.algorithms.depminerspark.model._AgreeSet;
import cz.cuni.matfyz.algorithms.depminerspark.model._CMAX_SET;
import cz.cuni.matfyz.algorithms.depminerspark.model._CSVTestCase;
import cz.cuni.matfyz.algorithms.depminerspark.model._FunctionalDependencyGroup;
import cz.cuni.matfyz.algorithms.depminerspark.model._MAX_SET;
import cz.cuni.matfyz.algorithms.depminerspark.model._StrippedPartition;
import cz.cuni.matfyz.algorithms.depminerspark.service._AgreeSetGenerator;
import cz.cuni.matfyz.algorithms.depminerspark.service._CMAX_SET_Generator;
import cz.cuni.matfyz.algorithms.depminerspark.service._FunctionalDependencyGenerator;
import cz.cuni.matfyz.algorithms.depminerspark.service._LeftHandSideGenerator;
import cz.cuni.matfyz.algorithms.depminerspark.service._StrippedPartitionGenerator;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.lucene.util.OpenBitSet;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

/**
 *
 * @author pavel.koupil
 */
public class DepMinerSpark {

//	private final int numberOfThreads;
    private final _CSVTestCase input;

    private _CMAX_SET_Generator setGenerator;

    Int2ObjectMap<List<BitSet>> lhss;

    _FunctionalDependencyGenerator xxx;

    public DepMinerSpark(/*int numberOfThreads,*/_CSVTestCase input) {
//		this.numberOfThreads = numberOfThreads;
        this.input = input;
    }

    public void execute() throws Exception {
        _StrippedPartitionGenerator spg = new _StrippedPartitionGenerator();
        JavaPairRDD<BitSet, _StrippedPartition> strippedPartitions = spg.execute(input);
        List<Tuple2<BitSet, _StrippedPartition>> lsp = strippedPartitions.collect();
        System.out.println("----- STRIPPED PARTITIONS -----");
        System.out.println("size: " + lsp.size());
		/*for (int index = 0; index < lsp.size(); ++index) {
			System.out.println(lsp.get(index));
		}
        */
        System.out.println("");

        int length = input.numberOfColumns();

        JavaRDD<_AgreeSet> agreeSets = new _AgreeSetGenerator().executeBottleneck(strippedPartitions);
        System.out.println("----- AGREE SET -----");
        List<_AgreeSet> listAG = agreeSets.collect();
        System.out.println("size: " + listAG.size());
            /*for (int index = 0; index < listAG.size(); ++index) {
                    System.out.println(listAG.get(index));
            }
        */
        System.out.println("");

        setGenerator = new _CMAX_SET_Generator(agreeSets, length);
        JavaPairRDD<Integer, _MAX_SET> maxSets = setGenerator.generateMaxSet();
        System.out.println("----- MAXIMAL SETS -----");
        List<Tuple2<Integer, _MAX_SET>> ms = maxSets.collect();
        List<_MAX_SET> lms = new ArrayList(maxSets.collectAsMap().values());
        System.out.println("size: " + ms.size());
            /*for (int index = 0; index < ms.size(); ++index) {
                System.out.println(ms.get(index));
            }*/
        System.out.println("");
        
        JavaPairRDD<Integer, _CMAX_SET> cmaxSets = setGenerator.generateCMAX_SETs();
        System.out.println("----- COMPLEMENTS OF MAXIMAL SETS -----");
        List<Tuple2<Integer, _CMAX_SET>> cms = cmaxSets.collect();
        System.out.println("size: " + cms.size());
          /*  for (int index = 0; index < cms.size(); ++index) {
                System.out.println(cms.get(index));
            }*/
        System.out.println("");
        List<_CMAX_SET> lc = new ArrayList(cmaxSets.collectAsMap().values());
        lhss = new _LeftHandSideGenerator().execute(lc, length);
        xxx = new _FunctionalDependencyGenerator(input, input.relationName(), input.columnNames(), lhss);
        List<_FunctionalDependencyGroup> result = xxx.execute();
        System.out.println("----- FUNCTIONAL DEPENDENCIES -----");
        System.out.println("size: " + result.size());
        Map<Integer, List<_FunctionalDependencyGroup>> fds = new TreeMap<>();
        for (int index = 0; index < length; ++index) {
            List<_FunctionalDependencyGroup> list = new ArrayList<>();
            fds.put(index, list);
        }
        for (int index = 0; index < result.size(); ++index) {
            _FunctionalDependencyGroup fd = result.get(index);
            //TODO
            List<_FunctionalDependencyGroup> list = fds.get(fd.getAttributeID());
            list.add(fd);
        }

        fds.forEach((key, list) -> {
            System.out.println("Attribute: " + key + " Size: " + list.size());
            for (int index = 0; index < list.size(); ++index) {
                System.out.println(list.get(index));
            }
        });
        System.out.println("");

        // BUILDING ARMSTRONG RELATION!
        ArmstrongRelationBuilder builder = new ArmstrongRelationBuilder();
        List<int[]> AR = builder.execute(lms);

//		int index = 1;
        for (int[] value : AR) {
//			if (index == 1) {
//				String firstRow = value.replace("1", String.valueOf(0));
//				System.out.println(firstRow);
//			}
//			value = value.replace("1", "" + index++);
            StringBuilder b = new StringBuilder();
            for (int index = 0; index < value.length; ++index) {
                b.append(value[index]);
                if (index != value.length - 1) {
                    b.append(", ");
                }
            }
            System.out.println(b);
        }
        System.out.println("SIZE: " + AR.size());
        //TODO
        /*
        var AR_RL = builder.realworldAR(AR, input);

        String outputFilePath = "armstrong.csv";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFilePath))) {
            for (var line : AR_RL) {
                for (int index = 0; index < line.size(); ++index) {
                    System.out.print(line.get(index));
                    writer.write(line.get(index));
                    if (index < line.size() - 1) {
                        writer.write(", ");
                        System.out.print(", ");
                    } else {
                        System.out.println("");
                    }
                }
                writer.newLine(); // Start a new line after each row
            }
            System.out.println("Data saved to " + outputFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        */

    }

}
