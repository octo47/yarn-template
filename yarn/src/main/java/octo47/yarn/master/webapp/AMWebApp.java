package octo47.yarn.master.webapp;

import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebApp;

public class AMWebApp extends WebApp implements AMParams{

  @Override
  public void setup() {
    bind(GenericExceptionHandler.class);
    route("/", AppController.class);
    route("/app", AppController.class);
  }
}
