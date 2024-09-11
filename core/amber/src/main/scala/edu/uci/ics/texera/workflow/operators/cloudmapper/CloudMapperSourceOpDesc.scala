package edu.uci.ics.texera.workflow.operators.cloudmapper

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.storage.DatasetFileDocument
import edu.uci.ics.amber.engine.common.workflow.OutputPort
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.source.PythonSourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.texera.workflow.operators.util.OperatorFilePathUtils

import java.nio.file.Paths

class CloudMapperSourceOpDesc extends PythonSourceOperatorDescriptor {
  @JsonProperty(required = true)
  @JsonSchemaTitle("FastQ Files")
  @JsonPropertyDescription("Zip file containing FASTQ files")
  val fastQFiles: String = ""

  @JsonProperty(required = true)
  @JsonSchemaTitle("Reference Genomes")
  @JsonPropertyDescription("Add one or more reference genomes")
  var referenceGenomes: List[ReferenceGenome] = List()

  @JsonProperty(required = true)
  @JsonSchemaTitle("Cluster Id")
  @JsonPropertyDescription("Cluster Id")
  val clusterId: String = ""

  override def generatePythonCode(): String = {
    // Convert the Scala fastQFiles into a file path
    val (filepath, fileDesc) = OperatorFilePathUtils.determineFilePathOrDatasetFile(Some(fastQFiles))
    val fastQFilePath = if (filepath != null) filepath else fileDesc.asFile().toPath.toString

    // Convert the Scala referenceGenomes list to a Python list format
    val pythonReferenceGenomes = referenceGenomes
      .map(_.referenceGenome.getName)
      .map(name => s"'$name'")
      .mkString("[", ", ", "]")

    // Convert the Scala referenceGenomes list to a Python list format for FASTA files
    val pythonFastaFiles = referenceGenomes
      .flatMap(_.fastAFiles)
      .map(file => {
        val (filepath, fileDesc) = OperatorFilePathUtils.determineFilePathOrDatasetFile(Some(file))
        val fastAFilePath = if (filepath != null) filepath else fileDesc.asFile().toPath.toString
        s"open(r'$fastAFilePath', 'rb')"
      })
      .mkString("[", ", ", "]")

    // Extract GTF file if exists for 'Others'
    val pythonGtfFile = referenceGenomes
      .find(_.referenceGenome == ReferenceGenomeEnum.OTHERS)
      .flatMap(_.gtfFile)
      .map(file => {
        val (filepath, fileDesc) = OperatorFilePathUtils.determineFilePathOrDatasetFile(Some(file))
        val gtfFilePath = if (filepath != null) filepath else fileDesc.asFile().toPath.toString
        s"open(r'$gtfFilePath', 'rb')"
      })
      .getOrElse("")

    s"""from pytexera import *
       |
       |class GenerateOperator(UDFSourceOperator):
       |
       |    @overrides
       |    def produce(self) -> Iterator[Union[TupleLike, TableLike, None]]:
       |        import requests, zipfile, io
       |
       |        # Set the URL to the Go endpoint
       |        url = "${AmberConfig.clusterLauncherServiceTarget}/api/job/create"
       |
       |        def create_job_form_data(cluster_id, job_form):
       |            form_data = {
       |                'cid': str(cluster_id)
       |            }
       |
       |            # Append the 'reads' file from the form control
       |            reads_file = job_form.get('reads')
       |            files = {}
       |            if reads_file:
       |                files['file'] = reads_file
       |
       |            # Append selected genomes and related files
       |            append_selected_genomes_to_form_data(form_data, files, job_form)
       |
       |            return form_data, files
       |
       |        def append_selected_genomes_to_form_data(form_data, files, job_form):
       |            selected_genomes = job_form.get('referenceGenome', [])
       |
       |            for index, genome in enumerate(selected_genomes):
       |                form_data[f'referenceGenome[{index}]'] = genome
       |
       |            if 'Others' in selected_genomes:
       |                append_fasta_and_gtf_files_to_form_data(files, job_form)
       |
       |        def append_fasta_and_gtf_files_to_form_data(files, job_form):
       |            fasta_files = job_form.get('fastaFiles', [])
       |            gtf_file = job_form.get('gtfFile')
       |
       |            for index, fasta_file in enumerate(fasta_files):
       |                files[f'fastaFiles[{index}]'] = fasta_file
       |
       |            if gtf_file:
       |                files['gtfFile'] = gtf_file
       |
       |        # Example job_form dictionary using inputs from Scala
       |        job_form = {
       |            'reads': open(r'${fastQFilePath}', 'rb'),
       |            'referenceGenome': ${pythonReferenceGenomes},
       |            'fastaFiles': ${pythonFastaFiles} if 'Others' in ${pythonReferenceGenomes} else [],
       |            'gtfFile': ${pythonGtfFile}
       |        }
       |
       |        # Create form data and files based on the provided job_form
       |        form_data, files = create_job_form_data(${clusterId}, job_form)
       |
       |        # Send the POST request to the Go program
       |        response = requests.post(url, data=form_data, files=files)
       |
       |        if response.status_code == 200:
       |            zip_file = zipfile.ZipFile(io.BytesIO(response.content))
       |
       |            features_content = zip_file.read('features.tsv')
       |            barcodes_content = zip_file.read('barcodes.tsv')
       |            matrix_content = zip_file.read('matrix.mtx')
       |            yield {'features': features_content, 'barcodes': barcodes_content, 'matrix': matrix_content}
       |        else:
       |            print(f"Failed to get the files. Status Code: {response.status_code}")
       |            print(f"Response Text: {response.text}")
       |            yield {'features': None, 'barcodes': None, 'matrix': None}
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
        new Attribute("features", AttributeType.BINARY),
        new Attribute("barcodes", AttributeType.BINARY),
        new Attribute("matrix", AttributeType.BINARY)
      )
      .build()
}
