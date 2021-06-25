package edu.uci.ics.texera.web.resource;

import edu.uci.ics.texera.web.TexeraWebApplication;
import edu.uci.ics.texera.web.TexeraWebConfiguration;
import io.dropwizard.client.JerseyClientBuilder;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by SmartGUI team on 5/5/17.
 */
public class SystemResourceTest {
	@ClassRule
	public static final DropwizardAppRule<TexeraWebConfiguration> RULE =
					new DropwizardAppRule<>(TexeraWebApplication.class, ResourceHelpers.resourceFilePath("test-config.yml"));
    public static Client client;

    @BeforeClass
    public static void setUpClient() {
        client = new JerseyClientBuilder(RULE.getEnvironment()).build("test client");
        client.property(ClientProperties.CONNECT_TIMEOUT, 5000);
        client.property(ClientProperties.READ_TIMEOUT,    5000);
    }
    
	@Test
	public void checkTableMetadata() throws Exception {
		Response response = client.target(
						String.format("http://localhost:%d/api/resources/table-metadata", RULE.getLocalPort()))
						.request()
						.get();

		assertThat(response.getStatus()).isEqualTo(200);
	}
}