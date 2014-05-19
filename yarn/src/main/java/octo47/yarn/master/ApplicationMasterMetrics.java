package octo47.yarn.master;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;

/**
 * @author Andrey Stepachev
 */
@Metrics(about = "Yarn Template Application Master Metrics", context = "yarn.template")
public class ApplicationMasterMetrics {

  public static ApplicationMasterMetrics create() {
    return create(DefaultMetricsSystem.instance());
  }

  public static ApplicationMasterMetrics create(MetricsSystem ms) {
    JvmMetrics.initSingleton("ApplicationMaster", null);
    return ms.register(new ApplicationMasterMetrics());
  }
}
