package edu.uci.ics.texera.workflow.operators.visualization

object ImageUtility {
  def encodeImageToHTML(): String = {
    s"""
       |        import base64
       |        try:
       |            encoded_image_data = base64.b64encode(binary_image_data)
       |            encoded_image_str = encoded_image_data.decode("utf-8")
       |        except Exception as e:
       |            yield {'html-content': self.render_error("Binary input is not valid")}
       |            return
       |        html = f'<img src="data:image;base64,{encoded_image_str}" alt="Image" style="max-width: 100vw; max-height: 90vh; width: auto; height: auto;">'
       |""".stripMargin
  }
}
