package octo47.yarn.master.webapp;

import org.apache.hadoop.yarn.webapp.WebApps;
import octo47.yarn.master.AppContext;

/**
 * @author Andrey Stepachev
 */
public class AMWebAppTestMain {

  public static void main(String[] args) {
    WebApps.$for("yarn-template", AppContext.class, new MockAppContext(1)).
            at(58888).inDevMode().start(new AMWebApp()).joinThread();
  }

}
