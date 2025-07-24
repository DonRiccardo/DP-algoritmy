package cz.cuni.mff.algorithms.hyfd_spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

//import de.metanome.algorithm_integration.AlgorithmConfigurationException;
//import de.metanome.algorithm_integration.AlgorithmExecutionException;
import cz.cuni.mff.algorithms.hyfd_spark.model._FunctionalDependency._ColumnCombination;
import cz.cuni.mff.algorithms.hyfd_spark.model._FunctionalDependency._ColumnIdentifier;
//import de.metanome.algorithm_integration.algorithm_types.BooleanParameterAlgorithm;
//import de.metanome.algorithm_integration.algorithm_types.FunctionalDependencyAlgorithm;
//import de.metanome.algorithm_integration.algorithm_types.IntegerParameterAlgorithm;
//import de.metanome.algorithm_integration.algorithm_types.RelationalInputParameterAlgorithm;
//import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
//import de.metanome.algorithm_integration.configuration.ConfigurationRequirementBoolean;
//import de.metanome.algorithm_integration.configuration.ConfigurationRequirementInteger;
//import de.metanome.algorithm_integration.configuration.ConfigurationRequirementRelationalInput;
//import de.metanome.algorithm_integration.input.InputGenerationException;
//import de.metanome.algorithm_integration.input.InputIterationException;
//import de.metanome.algorithm_integration.input.RelationalInput;
//import de.metanome.algorithm_integration.input.RelationalInputGenerator;
//import de.metanome.algorithm_integration.result_receiver.FunctionalDependencyResultReceiver;
import cz.cuni.mff.algorithms.hyfd_spark.model._FunctionalDependency;
//import cz.cuni.mff.algorithms.hyfd_spark.fdep.FDEP;
import cz.cuni.mff.algorithms.hyfd_spark.model.FDList;
import cz.cuni.mff.algorithms.hyfd_spark.model.FDSet;
import cz.cuni.mff.algorithms.hyfd_spark.model.FDTree;
import cz.cuni.mff.algorithms.hyfd_spark.model.IntegerPair;
import cz.cuni.mff.algorithms.hyfd_spark.services.PLIBuilder;
import cz.cuni.mff.algorithms.hyfd_spark.model.PositionListIndex;
import cz.cuni.mff.algorithms.hyfd_spark.model._CSVTestCase;
import cz.cuni.mff.algorithms.hyfd_spark.utils.Logger;
import cz.cuni.mff.algorithms.hyfd_spark.utils.ValueComparator;
import cz.cuni.mff.algorithms.hyfd_spark.services.MemoryGuardian;
import cz.cuni.mff.algorithms.hyfd_spark.services.Sampler;
import cz.cuni.mff.algorithms.hyfd_spark.services.Validator;
import cz.cuni.mff.algorithms.hyfd_spark.services.Inductor;
import de.uni_potsdam.hpi.utils.CollectionUtils;
//import de.uni_potsdam.hpi.utils.FileUtils;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.spark.api.java.JavaSparkContext;

public class HyFDSparkAlgorithm /*implements FunctionalDependencyAlgorithm, BooleanParameterAlgorithm, IntegerParameterAlgorithm, RelationalInputParameterAlgorithm */{

	public enum Identifier {
		INPUT_GENERATOR, NULL_EQUALS_NULL, VALIDATE_PARALLEL, ENABLE_MEMORY_GUARDIAN, MAX_DETERMINANT_SIZE, INPUT_ROW_LIMIT
	}

	//private RelationalInputGenerator inputGenerator = null;
        private _CSVTestCase input;
	//private FunctionalDependencyResultReceiver resultReceiver = null;

	private ValueComparator valueComparator;
	private final MemoryGuardian memoryGuardian = new MemoryGuardian(true);
	
	private boolean validateParallel = true;	// The validation is the most costly part in HyFD and it can easily be parallelized
	private int maxLhsSize = -1;				// The lhss can become numAttributes - 1 large, but usually we are only interested in FDs with lhs < some threshold (otherwise they would not be useful for normalization, key discovery etc.)
	private int inputRowLimit = -1;				// Maximum number of rows to be read from for analysis; values smaller or equal 0 will cause the algorithm to read all rows
	
	private float efficiencyThreshold = 0.01f;
	
	private String tableName;
	private List<String> attributeNames;
	private int numAttributes;
	private JavaSparkContext context;
        
    public HyFDSparkAlgorithm (_CSVTestCase input, JavaSparkContext context){
        this.input = input;
		this.context = context;
    }
	
/*	
	public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
		ArrayList<ConfigurationRequirement<?>> configs = new ArrayList<ConfigurationRequirement<?>>(5);
		configs.add(new ConfigurationRequirementRelationalInput(HyFD.Identifier.INPUT_GENERATOR.name()));
		
		ConfigurationRequirementBoolean nullEqualsNull = new ConfigurationRequirementBoolean(HyFD.Identifier.NULL_EQUALS_NULL.name());
		Boolean[] defaultNullEqualsNull = new Boolean[1];
		defaultNullEqualsNull[0] = new Boolean(true);
		nullEqualsNull.setDefaultValues(defaultNullEqualsNull);
		nullEqualsNull.setRequired(true);
		configs.add(nullEqualsNull);

		ConfigurationRequirementBoolean validateParallel = new ConfigurationRequirementBoolean(HyFD.Identifier.VALIDATE_PARALLEL.name());
		Boolean[] defaultValidateParallel = new Boolean[1];
		defaultValidateParallel[0] = new Boolean(this.validateParallel);
		validateParallel.setDefaultValues(defaultValidateParallel);
		validateParallel.setRequired(true);
		configs.add(validateParallel);

		ConfigurationRequirementBoolean enableMemoryGuardian = new ConfigurationRequirementBoolean(HyFD.Identifier.ENABLE_MEMORY_GUARDIAN.name());
		Boolean[] defaultEnableMemoryGuardian = new Boolean[1];
		defaultEnableMemoryGuardian[0] = new Boolean(this.memoryGuardian.isActive());
		enableMemoryGuardian.setDefaultValues(defaultEnableMemoryGuardian);
		enableMemoryGuardian.setRequired(true);
		configs.add(enableMemoryGuardian);
		
		ConfigurationRequirementInteger maxLhsSize = new ConfigurationRequirementInteger(HyFD.Identifier.MAX_DETERMINANT_SIZE.name());
		Integer[] defaultMaxLhsSize = new Integer[1];
		defaultMaxLhsSize[0] = new Integer(this.maxLhsSize);
		maxLhsSize.setDefaultValues(defaultMaxLhsSize);
		maxLhsSize.setRequired(false);
		configs.add(maxLhsSize);

		ConfigurationRequirementInteger inputRowLimit = new ConfigurationRequirementInteger(HyFD.Identifier.INPUT_ROW_LIMIT.name());
		Integer[] defaultInputRowLimit = { Integer.valueOf(this.inputRowLimit) };
		inputRowLimit.setDefaultValues(defaultInputRowLimit);
		inputRowLimit.setRequired(false);
		configs.add(inputRowLimit);
		
		return configs;
	}
*/
/*
	public void setResultReceiver(FunctionalDependencyResultReceiver resultReceiver) {
		this.resultReceiver = resultReceiver;
	}
*/
	public void setBooleanConfigurationValue(String identifier, Boolean... values) /*throws AlgorithmConfigurationException*/ {
		if (HyFDSparkAlgorithm.Identifier.NULL_EQUALS_NULL.name().equals(identifier))
			this.valueComparator = new ValueComparator(values[0].booleanValue());
		else if (HyFDSparkAlgorithm.Identifier.VALIDATE_PARALLEL.name().equals(identifier))
			this.validateParallel = values[0].booleanValue();
		else if (HyFDSparkAlgorithm.Identifier.ENABLE_MEMORY_GUARDIAN.name().equals(identifier))
			this.memoryGuardian.setActive(values[0].booleanValue());
		else
			this.handleUnknownConfiguration(identifier, CollectionUtils.concat(values, ","));
	}
	
	public void setIntegerConfigurationValue(String identifier, Integer... values) /*throws AlgorithmConfigurationException*/ {
		if (HyFDSparkAlgorithm.Identifier.MAX_DETERMINANT_SIZE.name().equals(identifier))
			this.maxLhsSize = values[0].intValue();
		else if (HyFDSparkAlgorithm.Identifier.INPUT_ROW_LIMIT.name().equals(identifier))
			if (values.length > 0)
				this.inputRowLimit = values[0].intValue();
		else {
			this.handleUnknownConfiguration(identifier, CollectionUtils.concat(values, ","));
		}
	}

    //public void setRelationalInputConfigurationValue(String identifier, RelationalInputGenerator... values) /*throws AlgorithmConfigurationException */{
    public void setRelationalInputConfigurationValue(_CSVTestCase inputCSV){    
        //if (HyFD.Identifier.INPUT_GENERATOR.name().equals(identifier))
        if (inputCSV != null)
            //this.inputGenerator = values[0];
            this.input = inputCSV;
        else
           // this.handleUnknownConfiguration(identifier, CollectionUtils.concat(values, ","));
            this.handleUnknownConfiguration("CSV input", "");
    }
	
    private void handleUnknownConfiguration(String identifier, String value) /*throws AlgorithmConfigurationException */{
        //throw new AlgorithmConfigurationException("Unknown configuration: " + identifier + " -> " + value);
        System.out.println("PROBLEM while handling configuration.");
    }

    @Override
    public String toString() {
        return "HyFD:\r\n\t" + 
                //"inputGenerator: " + ((this.inputGenerator != null) ? this.inputGenerator.toString() : "-") + "\r\n\t" +
                "tableName: " + this.tableName + " (" + CollectionUtils.concat(this.attributeNames, ", ") + ")\r\n\t" +
                "numAttributes: " + this.numAttributes + "\r\n\t" +
                "isNullEqualNull: " + ((this.valueComparator != null) ? String.valueOf(this.valueComparator.isNullEqualNull()) : "-") + ")\r\n\t" +
                "maxLhsSize: " + this.maxLhsSize + "\r\n" +
                "inputRowLimit: " + this.inputRowLimit + "\r\n" +
                "\r\n" +
                "Progress log: \r\n" + Logger.getInstance().read();
    }
	
    private void initialize(_CSVTestCase relationalInput) /*throws AlgorithmExecutionException */{
        this.tableName = relationalInput.relationName();
        this.attributeNames = relationalInput.columnNames();
        this.numAttributes = this.attributeNames.size();
        if (this.valueComparator == null)
            this.valueComparator = new ValueComparator(true);
    }
	
    public void execute() /*throws AlgorithmExecutionException*/ throws Exception{
        long startTime = System.currentTimeMillis();
        if (this.input == null){
           // throw new AlgorithmConfigurationException("No input generator set!");
            System.out.println("PROBLEM empty input.");
        }			
        /*if (this.resultReceiver == null){
           // throw new AlgorithmConfigurationException("No result receiver set!");
        }*/

        this.executeHyFD();

        Logger.getInstance().writeln("Time: " + (System.currentTimeMillis() - startTime) + " ms");
    }

	private void executeHyFD() /*throws AlgorithmExecutionException*/ throws Exception{
            // Initialize
            Logger.getInstance().writeln("Initializing ...");
		//RelationalInput relationalInput = this.getInput();
            _CSVTestCase relationalInput = this.input.generateNewCopy();
            this.initialize(relationalInput);
		
		///////////////////////////////////////////////////////
		// Build data structures for sampling and validation //
		///////////////////////////////////////////////////////
		
		// Calculate plis
		Logger.getInstance().writeln("Reading data and calculating plis ...");
		PLIBuilder pliBuilder = new PLIBuilder(this.inputRowLimit);
		List<PositionListIndex> plis = pliBuilder.getPLIs(relationalInput, this.numAttributes, this.valueComparator.isNullEqualNull());
		//this.closeInput(relationalInput);

		final int numRecords = pliBuilder.getNumLastRecords();
		pliBuilder = null;
		
		if (numRecords == 0) {
			ObjectArrayList<_ColumnIdentifier> columnIdentifiers = this.buildColumnIdentifiers();
			for (int attr = 0; attr < this.numAttributes; attr++)
				this.input.addResultToFile(new _FunctionalDependency(new _ColumnCombination(), columnIdentifiers.get(attr)));
			return;
		}
		
		// Sort plis by number of clusters: For searching in the covers and for validation, it is good to have attributes with few non-unique values and many clusters left in the prefix tree
		Logger.getInstance().writeln("Sorting plis by number of clusters ...");
		
                Collections.sort(plis, new Comparator<PositionListIndex>() {
			@Override
			public int compare(PositionListIndex o1, PositionListIndex o2) {		
				int numClustersInO1 = numRecords - o1.getNumNonUniqueValues() + o1.getClusters().size();
				int numClustersInO2 = numRecords - o2.getNumNonUniqueValues() + o2.getClusters().size();
				return numClustersInO2 - numClustersInO1;
			}
		});
		
		// Calculate inverted plis
		Logger.getInstance().writeln("Inverting plis ...");
		int[][] invertedPlis = this.invertPlis(plis, numRecords);

		// Extract the integer representations of all records from the inverted plis
		Logger.getInstance().writeln("Extracting integer representations for the records ...");
		int[][] compressedRecords = new int[numRecords][];
		for (int recordId = 0; recordId < numRecords; recordId++)
			compressedRecords[recordId] = this.fetchRecordFrom(recordId, invertedPlis);
		invertedPlis = null;
		
		// Initialize the negative cover
		FDSet negCover = new FDSet(this.numAttributes, this.maxLhsSize);
		
		// Initialize the positive cover
		FDTree posCover = new FDTree(this.numAttributes, this.maxLhsSize);
		posCover.addMostGeneralDependencies();
		
		//////////////////////////
		// Build the components //
		//////////////////////////

		// TODO: implement parallel sampling
		
		Sampler sampler = new Sampler(negCover, posCover, compressedRecords, plis, this.efficiencyThreshold, this.valueComparator, this.memoryGuardian);
		Inductor inductor = new Inductor(negCover, posCover, this.memoryGuardian);
		Validator validator = new Validator(negCover, posCover, numRecords, compressedRecords, plis, this.efficiencyThreshold, this.validateParallel, this.memoryGuardian, this.context);
		
		List<IntegerPair> comparisonSuggestions = new ArrayList<>();
		do {
                    
			FDList newNonFds = sampler.enrichNegativeCover(comparisonSuggestions);
			inductor.updatePositiveCover(newNonFds);
			comparisonSuggestions = validator.validatePositiveCover();
		}
		while (comparisonSuggestions != null);
		negCover = null;
		
		// Output all valid FDs
		Logger.getInstance().writeln("Translating FD-tree into result format ...");
		
	//	int numFDs = posCover.writeFunctionalDependencies("HyFD_backup_" + this.tableName + "_results.txt", this.buildColumnIdentifiers(), plis, false);
		int numFDs = posCover.addFunctionalDependenciesInto(this.input, this.buildColumnIdentifiers(), plis);
		
		Logger.getInstance().writeln("... done! (" + numFDs + " FDs)");
	}

/*
    private RelationalInput getInput() /*throws InputGenerationException, AlgorithmConfigurationException {
        RelationalInput relationalInput = this.inputGenerator.generateNewCopy();
        if (relationalInput == null)
            //throw new InputGenerationException("Input generation failed!");
        return relationalInput;
    }
*/
/*
	private void closeInput(RelationalInput relationalInput) {
		FileUtils.close(relationalInput);
	}
*/
	private ObjectArrayList<_ColumnIdentifier> buildColumnIdentifiers() {
		ObjectArrayList<_ColumnIdentifier> columnIdentifiers = new ObjectArrayList<_ColumnIdentifier>(this.attributeNames.size());
		for (String attributeName : this.attributeNames)
			columnIdentifiers.add(new _ColumnIdentifier(this.tableName, attributeName));
		return columnIdentifiers;
	}

/*
    private ObjectArrayList<List<String>> loadData(RelationalInput relationalInput) /*throws InputIterationException {
            ObjectArrayList<List<String>> records = new ObjectArrayList<List<String>>();
            while (relationalInput.hasNext())
                    records.add(relationalInput.next());
            return records;
    }
    */

    private int[][] invertPlis(List<PositionListIndex> plis, int numRecords) {
        int[][] invertedPlis = new int[plis.size()][];
        for (int attr = 0; attr < plis.size(); attr++) {
            int[] invertedPli = new int[numRecords];
            Arrays.fill(invertedPli, -1);

            for (int clusterId = 0; clusterId < plis.get(attr).size(); clusterId++) {
                for (int recordId : plis.get(attr).getClusters().get(clusterId))
                    invertedPli[recordId] = clusterId;
            }
            invertedPlis[attr] = invertedPli;
        }
        return invertedPlis;
    }
	
    private int[] fetchRecordFrom(int recordId, int[][] invertedPlis) {
        int[] record = new int[this.numAttributes];
        for (int i = 0; i < this.numAttributes; i++)
            record[i] = invertedPlis[i][recordId];
        return record;
    }
}
