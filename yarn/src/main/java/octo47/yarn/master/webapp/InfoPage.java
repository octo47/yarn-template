package octo47.yarn.master.webapp;

import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.view.InfoBlock;
import octo47.yarn.master.AppContext;

import javax.inject.Inject;

/**
 * @author Andrey Stepachev
 */
public class InfoPage extends AppView {

  @Inject
  public InfoPage(AppContext context) {
    super(context);
  }

  @Override
  protected void preHead(Page.HTML<_> html) {
    commonPreHead(html);
    setTitle("About the Application Master");
  }

  @Override
  protected Class<? extends SubView> content() {
    return InfoBlock.class;
  }
}
