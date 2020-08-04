package edu.uci.ics.texera.dataflow.udf;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.dataflow.annotation.AdvancedOption;
import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;
import javafx.util.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PythonUDFPredicate extends PredicateBase {

    private final String PythonScriptName;
    private final List<String> inputAttributeNames;
    private final List<String> resultAttributeNames;
    private final List<AttributeType> resultAttributeTypes;
    private final List<String> outerFileNames;
    private final int batchSize;
    private final int chunkSize;

    @JsonCreator
    public PythonUDFPredicate(
            @JsonProperty(value = PropertyNameConstants.PYTHON_UDF_SCRIPT_NAME, required = true)
                    String PythonScriptName,
            @AdvancedOption
            @JsonProperty(value = PropertyNameConstants.PYTHON_UDF_BATCH_SIZE, required = true, defaultValue = "10")
                    int batchSize,
            @JsonProperty(value = PropertyNameConstants.ARROW_CHUNK_SIZE, required = true, defaultValue = "10")
                    int chunkSize,
            @JsonProperty(value = PropertyNameConstants.PYTHON_UDF_INPUT_COLUMN_NAMES)
            List<String> inputAttributeNames,
            @JsonProperty(value = PropertyNameConstants.PYTHON_UDF_OUTPUT_COLUMN_NAMES)
            List<String> resultAttributeNames,
            @JsonProperty(value = PropertyNameConstants.PYTHON_UDF_OUTPUT_COLUMN_TYPES)
            List<AttributeType> resultAttributeTypes,
            @JsonProperty(value = PropertyNameConstants.PYTHON_UDF_FILE_NAMES)
            List<String> outerFileNames) {
        for (String s: inputAttributeNames) {
            if (s.isEmpty()) {
                throw new TexeraException("Input Attribute Name Cannot Be Empty");
            }
        }
        for (String s: resultAttributeNames) {
            if (s.isEmpty()) {
                throw new TexeraException("Result Attribute Name Cannot Be Empty");
            }
        }
        for (String s: outerFileNames) {
            if (s.isEmpty()) {
                throw new TexeraException("Out File Name Cannot Be Empty");
            }
        }
        if (PythonScriptName.isEmpty()) throw new TexeraException("Name of the Python Script Cannot Be Empty!");
        if (resultAttributeNames.size() != resultAttributeTypes.size()) {
            throw new TexeraException("Number of Result Field Names and Types Must Be consistent!");
        }
        this.PythonScriptName = PythonScriptName;
        this.inputAttributeNames = Collections.unmodifiableList(inputAttributeNames);
        this.outerFileNames = Collections.unmodifiableList(outerFileNames);
        this.resultAttributeNames = Collections.unmodifiableList(resultAttributeNames);
        this.resultAttributeTypes = Collections.unmodifiableList(resultAttributeTypes);
        this.batchSize = batchSize;
        this.chunkSize = chunkSize;
    };

    @JsonProperty(PropertyNameConstants.PYTHON_UDF_SCRIPT_NAME)
    public String getPythonScriptName() {
        return this.PythonScriptName;
    }

    @JsonProperty(PropertyNameConstants.PYTHON_UDF_INPUT_COLUMN_NAMES)
    public List<String> getInputAttributeNames() {
        return this.inputAttributeNames;
    }

    @JsonProperty(PropertyNameConstants.PYTHON_UDF_OUTPUT_COLUMN_NAMES)
    public List<String> getResultAttributeNames() {
        return this.resultAttributeNames;
    }

    @JsonProperty(PropertyNameConstants.PYTHON_UDF_OUTPUT_COLUMN_TYPES)
    public List<AttributeType> getResultAttributeTypes() {
        return resultAttributeTypes;
    }

    @JsonProperty(PropertyNameConstants.PYTHON_UDF_FILE_NAMES)
    public List<String> getOuterFileNames() {
        return this.outerFileNames;
    }

    @JsonProperty(PropertyNameConstants.PYTHON_UDF_BATCH_SIZE)
    public int getBatchSize() {
        return this.batchSize;
    }

    @JsonProperty(PropertyNameConstants.ARROW_CHUNK_SIZE)
    public int getChunkSize() {
        return this.chunkSize;
    }

    @Override
    public PythonUDFOperator newOperator() {
        return new PythonUDFOperator(this);
    }

    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
                .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Python UDF")
                .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "User-defined function operator by Python script.")
                .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.PYTHON_UDF_GROUP)
                .build();
    }

}
