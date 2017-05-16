package edu.uci.ics.textdb.web.resource;

import edu.uci.ics.textdb.web.TextdbWebApplication;
import edu.uci.ics.textdb.web.TextdbWebConfiguration;
import io.dropwizard.client.JerseyClientBuilder;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;
import org.glassfish.jersey.client.ClientProperties;
import org.junit.ClassRule;
import org.junit.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by SmartGUI team on 5/5/17.
 */
public class SystemResourceTest {
	@ClassRule
	public static final DropwizardAppRule<TextdbWebConfiguration> RULE =
					new DropwizardAppRule<>(TextdbWebApplication.class, ResourceHelpers.resourceFilePath("test-config.yml"));

	@Test
	public void checkSampleEndpoint() throws Exception {
		Client client = new JerseyClientBuilder(RULE.getEnvironment()).build("test client");
		client.property(ClientProperties.CONNECT_TIMEOUT, 5000);
		client.property(ClientProperties.READ_TIMEOUT, 5000);

		Response response = client.target(
						String.format("http://localhost:%d/api/resources/metadata", RULE.getLocalPort()))
						.request()
						.get();

		assertThat(response.getStatus()).isEqualTo(200);

	}

}