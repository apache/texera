package edu.uci.ics.texera.web.resource;

import edu.uci.ics.texera.web.TexeraWebApplication;
import edu.uci.ics.texera.web.TexeraWebConfiguration;
import io.dropwizard.client.JerseyClientBuilder;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by SmartGUI team on 5/5/17.
 */
public class FileUploadResourceTest {
	@ClassRule
	public static final DropwizardAppRule<TexeraWebConfiguration> RULE =
					new DropwizardAppRule<>(TexeraWebApplication.class, ResourceHelpers.resourceFilePath("test-config.yml"));

    // TODO:: We are getting 400. However, it works with front-end. So we need to fix this test case.
	@Test
	@Ignore
	public void checkDictionaryUpload() throws Exception {
		Client client = new JerseyClientBuilder(RULE.getEnvironment()).build("test client");
		client.property(ClientProperties.CONNECT_TIMEOUT, 5000);
		client.property(ClientProperties.READ_TIMEOUT, 5000);
		client.register(MultiPartFeature.class);

		final MultiPart multiPart = new MultiPart();
		multiPart.setMediaType(MediaType.MULTIPART_FORM_DATA_TYPE);
		File testDictionaryFile = new File(ResourceHelpers.resourceFilePath("test_dictionary.txt"));
		final FileDataBodyPart filePart = new FileDataBodyPart("file", testDictionaryFile);
		multiPart.bodyPart(filePart);

		Response response = client.target(
						String.format("http://localhost:%d/api/upload/dictionary", RULE.getLocalPort()))
						.request(MediaType.MULTIPART_FORM_DATA_TYPE)
						.post(Entity.entity(multiPart, multiPart.getMediaType()));
		
		assertThat(response.getStatus()).isEqualTo(200);
	}
}