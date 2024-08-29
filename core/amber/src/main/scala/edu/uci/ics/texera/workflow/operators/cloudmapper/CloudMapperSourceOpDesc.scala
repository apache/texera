package edu.uci.ics.texera.workflow.operators.cloudmapper

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.common.workflow.OutputPort
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.source.PythonSourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}

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
    s"""from pytexera import *
           |
           |class GenerateOperator(UDFSourceOperator):
           |
           |    @overrides
           |    def produce(self) -> Iterator[Union[TupleLike, TableLike, None]]:
           |        import requests
           |
           |        # Set the URL to the Go endpoint
           |        url = "http://localhost:3000/api/job/create"
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
           |        # Example job_form dictionary, replace with your actual data
           |        job_form = {
           |            'reads': open('$fastQFiles', 'rb'),  # Replace with actual file path
           |            'referenceGenome': ['Others'],
           |            'fastaFiles': [
           |                open('/Users/kunwoopark/Downloads/NC_001802.fna', 'rb'),  # Replace with actual file path
           |            ],
           |            'gtfFile': open('/Users/kunwoopark/Downloads/NC_001802.gtf', 'rb')  # Replace with actual file path
           |        }
           |        # Create form data and files based on the provided job_form
           |        form_data, files = create_job_form_data($clusterId, job_form)
           |
           |        # Send the POST request to the Go program
           |        response = requests.post(url, data=form_data, files=files)
           |
           |        # Print the response from the Go program
           |        print(f"Response Status Code: {response.status_code}")
           |        print(f"Response Text: {response.text}")
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
