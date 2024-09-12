package edu.uci.ics.texera.workflow.operators.cloudmapper

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.workflow.OutputPort
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.source.PythonSourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.texera.workflow.operators.util.OperatorFilePathUtils


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
    val (filepath, fileDesc) =
      OperatorFilePathUtils.determineFilePathOrDatasetFile(Some(fastQFiles))
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
       |        import requests, io, time
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
       |        # Send the POST request to start the job
       |        response = requests.post("${AmberConfig.clusterLauncherServiceTarget}/api/job/create",
       |                                 data=form_data, files=files)
       |
       |        # Extract the job ID from the initial response
       |        job_id = response.json().get("job_id")
       |
       |        # Poll until the job is finished
       |        while True:
       |            # Poll the status endpoint
       |            status_response = requests.get(f'${AmberConfig.clusterLauncherServiceTarget}/api/job/status/{job_id}')
       |            status = status_response.json().get("status")
       |
       |            if status == "finished":
       |                print("Job finished! Downloading the result...")
       |                break
       |            elif status == "failed":
       |                print("Job failed.")
       |                yield {'features': None, 'barcodes': None, 'matrix': None}
       |                return
       |
       |            print("Job is still processing...")
       |            time.sleep(0.5)
       |            yield
       |
       |        # Request to download files after the job is completed
       |        download_response = requests.get(f'${AmberConfig.clusterLauncherServiceTarget}/api/job/download/{job_id}',
       |                                         params={'cid': str(${clusterId})})
       |
       |        if download_response.status_code == 200:
       |            # Read the directory path from the response text
       |            directory_path = download_response.text.strip()
       |            yield {'matrix_path': directory_path}
       |        else:
       |            print(f"Failed to get the files. Status Code: {download_response.status_code}")
       |            print(f"Response Text: {download_response.text}")
       |            yield {'matrix_path': None}
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
        new Attribute("matrix_path", AttributeType.STRING)
      )
      .build()
}
