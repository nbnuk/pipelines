package uk.org.nbn.kvs.cache;

import au.org.ala.kvs.ALAPipelinesConfig;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.cache.KeyValueCache;
import org.gbif.kvs.hbase.Command;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import uk.org.nbn.kvs.client.DataResourceNBN;
import uk.org.nbn.kvs.client.NBNCollectoryService;
import uk.org.nbn.kvs.client.retrofit.NBNCollectoryServiceClient;

import java.util.concurrent.TimeUnit;

/** Key value store factory for Attribution */
@Slf4j
public class DataResourceNBNKVStoreFactory {

  private final KeyValueStore<String, DataResourceNBN> kvStore;
  private static volatile DataResourceNBNKVStoreFactory instance;
  private static final Object MUTEX = new Object();

  @SneakyThrows
  private DataResourceNBNKVStoreFactory(ALAPipelinesConfig config) {
    this.kvStore = create(config);
  }

  public static KeyValueStore<String, DataResourceNBN> getInstance(
      ALAPipelinesConfig config) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new DataResourceNBNKVStoreFactory(config);
        }
      }
    }
    return instance.kvStore;
  }

  /** Retrieve KV Store for Collectory Metadata. */
  public static KeyValueStore<String, DataResourceNBN> create(ALAPipelinesConfig config) {
      NBNCollectoryServiceClient wsClient = new NBNCollectoryServiceClient(config.getCollectory());
    Command closeHandler =
        () -> {
          try {
            wsClient.close();
          } catch (Exception e) {
            log.error("Unable to close", e);
          }
        };

    return cache2kBackedKVStore(wsClient, closeHandler, config);
  }

  /** Builds a KV Store backed by the rest client. */
  private static KeyValueStore<String, DataResourceNBN> cache2kBackedKVStore(
          NBNCollectoryService service, Command closeHandler, ALAPipelinesConfig config) {

    KeyValueStore<String, DataResourceNBN> kvs =
        new KeyValueStore<String, DataResourceNBN>() {
          @Override
          public DataResourceNBN get(String key) {

            for (int i = 0; i < config.getCollectory().getRetryConfig().getMaxAttempts(); i++) {
              try {
                return service.lookupDataResourceNBN(key);
              } catch (retrofit2.HttpException ex) {
                log.error("HttpException looking up metadata for " + key, ex);
              } catch (Exception ex) {
                log.error("Exception looking up metadata for " + key, ex);
              }
              try {
                TimeUnit.MILLISECONDS.sleep(
                    config.getCollectory().getRetryConfig().getInitialIntervalMillis());
              } catch (Exception e) {
                //
              }
            }
            throw new PipelinesException("Unable to retrieve metadata for " + key);
          }

          @Override
          public void close() {
            closeHandler.execute();
          }
        };
    return KeyValueCache.cache(
        kvs, config.getCollectory().getCacheSizeMb(), String.class, DataResourceNBN.class);
  }

  public static SerializableSupplier<KeyValueStore<String, DataResourceNBN>>
      getInstanceSupplier(ALAPipelinesConfig config) {
    return () -> getInstance(config);
  }
}
