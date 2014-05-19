package octo47.yarn.master.webapp;

import com.google.inject.Injector;
import org.apache.hadoop.yarn.webapp.test.WebAppTests;
import org.junit.Assert;
import org.junit.Test;
import octo47.yarn.master.AppContext;

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.APP_ID;

public class AMWebAppTest {

  AppContext ctx = new MockAppContext(1);

  @Test
  public void simpleTest() {
    Injector injector = WebAppTests.createMockInjector(AppContext.class, ctx);
    AppController controller = injector.getInstance(AppController.class);
    controller.index();
    Assert.assertEquals(ctx.getApplicationId().toString(), controller.get(APP_ID, ""));

  }

  @Test
  public void testAppView() {
    WebAppTests.testPage(AppView.class, AppContext.class, new MockAppContext(1));
  }
}
