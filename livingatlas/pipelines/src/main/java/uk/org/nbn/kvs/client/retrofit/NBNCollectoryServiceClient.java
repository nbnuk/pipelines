package uk.org.nbn.kvs.client.retrofit;

import au.org.ala.utils.WsUtils;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import org.gbif.pipelines.core.config.model.WsConfig;
import retrofit2.HttpException;
import uk.org.nbn.kvs.client.NBNCollectoryService;
import uk.org.nbn.kvs.client.DataResourceNBN;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Objects;
import java.util.stream.Stream;

import static org.gbif.rest.client.retrofit.SyncCall.syncCall;

@Slf4j
/** Collectory service client implementation. */
public class NBNCollectoryServiceClient implements NBNCollectoryService {

  private final NBNCollectoryRetrofitService nbnCollectoryService;

  private final OkHttpClient okHttpClient;

  /** Creates an instance using the provided configuration settings. */
  public NBNCollectoryServiceClient(WsConfig config) {
    okHttpClient = WsUtils.createOKClient(config);
    nbnCollectoryService =
        WsUtils.createClient(okHttpClient, config, NBNCollectoryRetrofitService.class);
  }

  /**
   * Retrieve collectory metadata
   *
   * @param dataResourceUid data resource UID
   */
  @Override
  public DataResourceNBN lookupDataResourceNBN(String dataResourceUid) {
    try {
      return syncCall(nbnCollectoryService.lookupDataResourceNBN(dataResourceUid));
    }
    catch (HttpException e){
      if (e.code() == 404) {
        return DataResourceNBN.EMPTY;
      }
      log.error("Exception thrown when calling the collectory. " + e.getMessage(), e);
      return DataResourceNBN.EMPTY;
    }
    catch (Exception e) {
      // this necessary due to collectory returning 500s
      // instead of 404s
      log.error("Exception thrown when calling the collectory. " + e.getMessage(), e);
      return DataResourceNBN.EMPTY;
    }
  }


  @Override
  public void close() throws IOException {
    if (Objects.nonNull(okHttpClient) && Objects.nonNull(okHttpClient.cache())) {
      File cacheDirectory = okHttpClient.cache().directory();
      if (cacheDirectory.exists()) {
        try (Stream<File> files =
            Files.walk(cacheDirectory.toPath())
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)) {
          files.forEach(File::delete);
        }
      }
    }
  }
}
