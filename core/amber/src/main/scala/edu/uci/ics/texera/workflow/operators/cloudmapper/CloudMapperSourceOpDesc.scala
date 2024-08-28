  package edu.uci.ics.texera.workflow.operators.cloudmapper

  import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
  import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaString, JsonSchemaTitle}
  import edu.uci.ics.amber.engine.common.workflow.OutputPort
  import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
  import edu.uci.ics.texera.workflow.common.metadata.annotations.HideAnnotation
  import edu.uci.ics.texera.workflow.common.operators.source.PythonSourceOperatorDescriptor
  import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}

  class CloudMapperSourceOpDesc extends PythonSourceOperatorDescriptor {
    @JsonProperty(defaultValue = "false")
    @JsonSchemaTitle("Human Genome (GRCh38)")
    @JsonPropertyDescription("GRCh38 (2024-A)")
    val referenceGenome1: Boolean = false

    @JsonProperty(defaultValue = "false")
    @JsonSchemaTitle("Human Genome (hg19)")
    @JsonPropertyDescription("hg19 (3.0.0)")
    val referenceGenome2: Boolean = false

    @JsonProperty(defaultValue = "false")
    @JsonSchemaTitle("Mouse Genome (GRCh38)")
    @JsonPropertyDescription("GRCm38 (2024-A)")
    val referenceGenome3: Boolean = false

    @JsonProperty(defaultValue = "false")
    @JsonSchemaTitle("Mouse Genome (mm10)")
    @JsonPropertyDescription("mm10 (2020-A)")
    val referenceGenome4: Boolean = false

    @JsonProperty(defaultValue = "false")
    @JsonSchemaTitle("Others")
    @JsonPropertyDescription("Other reference genome")
    val others: Boolean = false

    @JsonSchemaTitle("FastA Files")
    @JsonSchemaInject(
      strings = Array(
        new JsonSchemaString(path = HideAnnotation.hideTarget, value = "others"),
        new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.equals),
        new JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "false")
      )
    )
    val fastAFiles: Option[String] = None

    @JsonSchemaTitle("Gtf File")
    @JsonSchemaInject(
      strings = Array(
        new JsonSchemaString(path = HideAnnotation.hideTarget, value = "others"),
        new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.equals),
        new JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "false")
      )
    )
    val gtfFile: Option[String] = None

    @JsonProperty(required = true)
    @JsonSchemaTitle("FastQ Files")
    @JsonPropertyDescription("Zip file containing FASTQ files")
    val fastQFiles: Option[String] = None

    @JsonProperty(required = true)
    @JsonSchemaTitle("Cluster Id")
    @JsonPropertyDescription("Cluster Id")
    val clusterId: Option[String] = None

    // Custom validation logic in constructor
    require(!(others && fastAFiles.isEmpty), "FastA Files must be provided if 'Others' is selected.")
    require(!(others && gtfFile.isEmpty), "GTF File must be provided if 'Others' is selected.")

    override def generatePythonCode(): String = {
        s"""from pytexera import *
           |
           |class GenerateOperator(UDFSourceOperator):
           |
           |    @overrides
           |    def produce(self) -> Iterator[Union[TupleLike, TableLike, None]]:
           |        yield {'response': 200}
           """.stripMargin
      }
      override def operatorInfo: OperatorInfo =
        OperatorInfo(
          "CloudMapper",
          "Running sequence alignment using public cluster services",
          OperatorGroupConstants.API_GROUP,
          inputPorts = List.empty,
          outputPorts = List(OutputPort())
        )
      override def asSource() = true
      override def sourceSchema(): Schema =
        Schema
          .builder()
          .add(
            new Attribute("response", AttributeType.INTEGER)
          )
          .build()
  }
