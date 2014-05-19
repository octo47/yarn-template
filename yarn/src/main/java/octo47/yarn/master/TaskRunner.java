package octo47.yarn.master;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import octo47.yarn.master.spec.TaskSpec;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * @author Andrey Stepachev
 */
public class TaskRunner {

  private final static Logger LOG = LoggerFactory.getLogger(TaskRunner.class);
  public static final Charset UTF8 = Charset.forName("UTF-8");

  public TaskRunner(File baseDir, TaskSpec spec) throws IOException {
    // only unix, sorry.
    baseDir.mkdirs();
    LOG.info("Using base dir: " + baseDir.getAbsolutePath());
    final File script =
            Shell.appendScriptExtension(baseDir, "yarn-template-runner");
    final TaskShellExecutor executor =
            new TaskShellExecutor(script, spec.getEnv(), spec.getCommands());
    executor.execute();
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      throw new IllegalArgumentException("Expected one argument with task spec");
    }
    final TaskSpec spec = new ObjectMapper()
            .reader(TaskSpec.class)
            .readValue(Base64.decodeBase64(args[0]));
    final Path remoteOutputDir = new Path(args[1]);
    LOG.info("Running spec:" + spec.toString());
    new TaskRunner(new File("."), spec);
    final List<String> outputs = spec.getOutputs();
    if (!outputs.isEmpty()) {
      final Configuration conf = new Configuration();
      final FileSystem fs = FileSystem.get(conf);
      for (String outputDir : outputs) {
        final String[] list = new File(outputDir).list();
        for (String f : list) {
          fs.copyFromLocalFile(
                  new Path(outputDir, f),
                  new Path(remoteOutputDir, f));
        }
      }
      LOG.info("Complete, gathering outputs: " + outputs);
    }
  }

  @VisibleForTesting
  static class TaskShellExecutor extends Shell {

    private final String[] runScriptCommand;

    TaskShellExecutor(File script, Map<String, String> env, List<String> commands) throws IOException {
      this.setEnvironment(env);
      if (Shell.WINDOWS)
        throw new IllegalStateException("Can't run on Windows yet");

      runScriptCommand = Shell.getRunScriptCommand(script);
      final UnixShellScriptBuilder builder = new UnixShellScriptBuilder();
      for (String command : commands) {
        builder.command(command);
      }
      for (String envKey : env.keySet()) {
        builder.env(envKey, env.get(envKey));
      }
      final FileWriter fw = new FileWriter(script);
      try {
        builder.write(fw);
      } finally {
        fw.close();
      }
    }

    public void execute() throws IOException {
      run();
    }

    @Override
    protected String[] getExecString() {
      return runScriptCommand;
    }


    protected void parseExecResult(BufferedReader lines) throws IOException {
      char[] buf = new char[512];
      final OutputStreamWriter writer =
              new OutputStreamWriter(System.out, UTF8);
      int nRead;
      while ((nRead = lines.read(buf, 0, buf.length)) > 0) {
        writer.write(buf, 0, nRead);
      }
      writer.flush();
    }
  }

  private static abstract class ShellScriptBuilder {

    private static final String LINE_SEPARATOR =
            System.getProperty("line.separator");
    private final StringBuilder sb = new StringBuilder();

    public abstract void command(String command);

    public abstract void env(String key, String value);

    public final void symlink(Path src, Path dst) throws IOException {
      if (!src.isAbsolute()) {
        throw new IOException("Source must be absolute");
      }
      if (dst.isAbsolute()) {
        throw new IOException("Destination must be relative");
      }
      if (dst.toUri().getPath().indexOf('/') != -1) {
        mkdir(dst.getParent());
      }
      link(src, dst);
    }

    @Override
    public String toString() {
      return sb.toString();
    }

    public final void write(Writer out) throws IOException {
      out.append(sb);
      out.flush();
    }

    protected final void line(String... command) {
      for (String s : command) {
        sb.append(s);
      }
      sb.append(LINE_SEPARATOR);
    }

    protected abstract void link(Path src, Path dst) throws IOException;

    protected abstract void mkdir(Path path);
  }

  private static final class UnixShellScriptBuilder extends ShellScriptBuilder {

    public UnixShellScriptBuilder() {
      line("#!/bin/bash");
      line();
    }

    @Override
    public void command(String command) {
      line("/bin/bash -c \"", command, "\"");
    }

    @Override
    public void env(String key, String value) {
      line("export ", key, "=\"", value, "\"");
    }

    @Override
    protected void link(Path src, Path dst) throws IOException {
      line("ln -sf \"", src.toUri().getPath(), "\" \"", dst.toString(), "\"");
    }

    @Override
    protected void mkdir(Path path) {
      line("mkdir -p ", path.toString());
    }
  }

}
