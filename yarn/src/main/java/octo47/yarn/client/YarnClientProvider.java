package octo47.yarn.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import javax.inject.Provider;

/**
 * @author Andrey Stepachev
 */
public class YarnClientProvider implements Provider<YarnClient> {

  private final Configuration configuration;

  public YarnClientProvider(Configuration configuration) {
    this.configuration = configuration;
  }

  @Override
  public YarnClient get() {
    final YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(new YarnConfiguration(configuration));
    yarnClient.start();
    return yarnClient;
  }
}
