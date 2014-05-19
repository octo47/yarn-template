package octo47.yarn.util;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import octo47.yarn.Constant;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Andrey Stepachev
 */
public class LocalizerHelper {

  private static final Log LOG = LogFactory.getLog(LocalizerHelper.class);

  private final Configuration conf;
  private final Path appStagingPath;

  private final Map<String, LocalResource> resources = Maps.newTreeMap();
  private final Map<String, LocalResource> distCache = Maps.newTreeMap();
  private static Function<LocalResource, MyLocalResource> myLocalResourceF =
          new Function<LocalResource, MyLocalResource>() {
            @Override
            public MyLocalResource apply(@Nullable LocalResource input) {
              if (input == null)
                return null;
              final MyLocalResource rv = new MyLocalResource();
              rv.setPattern(input.getPattern());
              final URL resource = input.getResource();
              try {
                rv.setResource(new URI(resource.getScheme(), resource.getUserInfo(),
                        resource.getHost(),resource.getPort(),
                        resource.getFile(), null, null));
              } catch (URISyntaxException e) {
                throw new RuntimeException(e);
              }
              rv.setSize(input.getSize());
              rv.setTimestamp(input.getTimestamp());
              rv.setType(input.getType());
              rv.setVisibility(input.getVisibility());
              return rv;
            }
          };

  private static Function<MyLocalResource, LocalResource> localResourceF =
          new Function<MyLocalResource, LocalResource>() {
            @Override
            public LocalResource apply(@Nullable MyLocalResource input) {
              if (input == null)
                return null;
              final URI uri = input.getResource();
              URL resource = URL.newInstance(
                      uri.getScheme(),
                      uri.getHost(),
                      uri.getPort(),
                      uri.getRawPath());
              resource.setUserInfo(uri.getRawUserInfo());
              return LocalResource.newInstance(
                      resource, input.getType(),
                      input.getVisibility(), input.getSize(),
                      input.getTimestamp(), input.getPattern()
              );
            }
          };


  public LocalizerHelper(Configuration conf, Path appStagingPath) throws IOException {
    this.conf = conf;
    this.appStagingPath = appStagingPath;
    final FileSystem fs = FileSystem.get(conf);
    fs.mkdirs(appStagingPath);
  }

  public LocalizerHelper(Configuration conf, String appStagingDir) throws IOException {
    this(conf, FileSystem.get(conf)
            .makeQualified(new Path(appStagingDir)));
  }

  public Path getAppStagingPath() {
    return appStagingPath;
  }

  public Map<String, LocalResource> getResources() {
    return resources;
  }

  public Map<String, LocalResource> getDistCache() {
    return distCache;
  }

  public void cleanup() throws IOException {
    final FileSystem fs = FileSystem.get(conf);
    fs.delete(appStagingPath, true);
  }

  public void addLocalFiles(Map<String, String> files)
          throws IOException {
    LOG.info("Preparing local resources");

    final FileSystem fs = FileSystem.get(conf);

    for (String remoteName : files.keySet()) {
      String localPath = files.get(remoteName);
      URI localURI = qualifyWithLocal(localPath);
      boolean setPermissions = remoteName.equals(Constant.APP_JAR_NAME);
      Path destPath = copyRemoteFile(appStagingPath, new Path(localURI),
              Constant.APP_RESOURCE_DEFAULT_REPLICATION, setPermissions);
      addResources(fs, destPath, remoteName, LocalResourceType.FILE);
    }
  }

  public void addLocalFile(String link, String localPath, boolean masterOnly) throws IOException {
    URI localURI = qualifyWithLocal(localPath);
    boolean setPermissions = link.equals(Constant.APP_JAR_NAME);
    Path destPath = copyRemoteFile(appStagingPath, new Path(localURI),
            Constant.APP_RESOURCE_DEFAULT_REPLICATION, setPermissions);
    addResources(FileSystem.get(conf), destPath, link, LocalResourceType.FILE, masterOnly);
  }

  public void addLocalFileMasterOnly(String link, String localPath) throws IOException {
    addLocalFile(link, localPath, true);
  }

  public void addLocalFileDistCache(String link, String localPath) throws IOException {
    addLocalFile(link, localPath, false);
  }

  private URI qualifyWithLocal(String localPath) throws IOException {
    URI localURI = getUri(localPath);
    if (localURI.getScheme() == null) {
      final LocalFileSystem localFs = FileSystem.getLocal(conf);
      localURI = getUri(localFs
              .makeQualified(new Path(localPath)).toString());
    }
    return localURI;
  }

  public void addResources(FileSystem fs,
                           Path destPath,
                           String link,
                           LocalResourceType type) throws IOException {
    addResources(fs, destPath, link, type, false);
  }

  public void addResources(FileSystem fs,
                           Path destPath,
                           String link,
                           LocalResourceType type,
                           boolean appMasterOnly) throws IOException {

    final FileStatus destStatus = fs.getFileStatus(destPath);
    final LocalResource localResource = Records.newRecord(LocalResource.class);
    localResource.setType(type);
    localResource.setVisibility(LocalResourceVisibility.APPLICATION); // TODO: Make configurable
    localResource.setResource(ConverterUtils.getYarnUrlFromPath(destPath));
    localResource.setTimestamp(destStatus.getModificationTime());
    localResource.setSize(destStatus.getLen());
    resources.put(link, localResource);

    if (!appMasterOnly) {
      switch (type) {
        case FILE:
          distCache.put(link, localResource);
          break;
        case ARCHIVE:
          distCache.put(link, localResource);
          break;
        case PATTERN:
          throw new IllegalArgumentException("PATTERN not supported yet");
        default:
          throw new IllegalArgumentException("Unknown type " + type);
      }
    }
  }

  public Path copyRemoteFile(Path remotePath, Path localPath, short replication, boolean setPerms) throws IOException {
    FileSystem remoteFs = FileSystem.get(conf);
    FileSystem localFs = localPath.getFileSystem(conf);
    Path newPath = localPath;
    if (!sameFs(remoteFs, localFs)) {
      newPath = new Path(remotePath, localPath.getName());
      LOG.info("Uploading " + localPath + " to " + newPath);
      FileUtil.copy(localFs, localPath, remoteFs, newPath, false, conf);
      remoteFs.setReplication(newPath, replication);
      if (setPerms)
        remoteFs.setPermission(newPath, new FsPermission((short) Constant.APP_JAR_PERMISSIONS));
    }
    // Resolve any symlinks in the URI path so using a "current" symlink to point to a specific
    // version shows the specific version in the distributed cache configuration
    Path qualPath = remoteFs.makeQualified(newPath);
    FileContext fc = FileContext.getFileContext(qualPath.toUri(), conf);
    return fc.resolvePath(qualPath);
  }

  public void serializeDistCace(Map<String, String> env) {
    final HashMap<String, MyLocalResource> map = Maps.newHashMap();
    for (Map.Entry<String, LocalResource> entry : distCache.entrySet()) {
      map.put(entry.getKey(), myLocalResourceF.apply(entry.getValue()));
    }
    env.put(Constant.APP_DIST_CACHE, serialize(map));
  }

  public static Map<String, LocalResource> deserializeDistCache(Map<String, String> env) {
    final String distCacheStr = env.get(Constant.APP_DIST_CACHE);
    if (distCacheStr == null)
      return Maps.newHashMap();
    else {
      final Map<String, MyLocalResource> distCache = deserialize(distCacheStr);
      final HashMap<String, LocalResource> map = Maps.newHashMap();
      for (Map.Entry<String, MyLocalResource> entry : distCache.entrySet()) {
        map.put(entry.getKey(), localResourceF.apply(entry.getValue()));
      }
      return map;
    }
  }

  private static URI getLinkUri(Path destPath, String link) throws IOException {
    URI destUri = destPath.toUri();
    try {
      return new URI(destUri.getScheme(), destUri.getAuthority(), destUri.getPath(), null, link);
    } catch (URISyntaxException e) {
      throw new IOException("Wrong link uri produced: " + destPath + " -> " + link, e);
    }
  }

  private static URI getUri(String localFile) throws IOException {
    try {
      return new URI(localFile);
    } catch (URISyntaxException e) {
      throw new IOException("Wrong file uri specified: " + localFile, e);
    }
  }

  public static boolean sameFs(FileSystem src, FileSystem dst) throws IOException {
    final URI srcUri = NetUtils.getCanonicalUri(src.getUri(), 0);
    final URI dstUri = NetUtils.getCanonicalUri(dst.getUri(), 0);
    return srcUri.equals(dstUri);
  }

  public static class MyLocalResource implements Serializable {

    private URI resource;
    private long size;
    private long timestamp;
    private LocalResourceType type;
    private LocalResourceVisibility visibility;
    private String pattern;

    public URI getResource() {
      return resource;
    }

    public void setResource(URI resource) {
      this.resource = resource;
    }

    public long getSize() {
      return size;
    }

    public void setSize(long size) {
      this.size = size;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public void setTimestamp(long timestamp) {
      this.timestamp = timestamp;
    }

    public LocalResourceType getType() {
      return type;
    }

    public void setType(LocalResourceType type) {
      this.type = type;
    }

    public LocalResourceVisibility getVisibility() {
      return visibility;
    }

    public void setVisibility(LocalResourceVisibility visibility) {
      this.visibility = visibility;
    }

    public String getPattern() {
      return pattern;
    }

    public void setPattern(String pattern) {
      this.pattern = pattern;
    }
  }

  public static <T> String serialize(Map<String, T> mapping) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(mapping);
      oos.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return Base64.encodeBase64String(baos.toByteArray());
  }

  public static <T> Map<String, T> deserialize(String serialized) {
    byte[] data = Base64.decodeBase64(serialized);
    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    Map<String, T> mapping;
    try {
      ObjectInputStream ois = new ObjectInputStream(bais);
      mapping = (Map<String, T>) ois.readObject();
      ois.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return mapping;
  }
}
