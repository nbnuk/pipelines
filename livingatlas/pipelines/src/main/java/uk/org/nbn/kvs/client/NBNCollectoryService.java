package uk.org.nbn.kvs.client;

import java.io.Closeable;
import retrofit2.http.Path;

/** An interface for the collectory web services */
public interface NBNCollectoryService extends Closeable {

  /** Retrieve the details of a data resource. */
  DataResourceNBN lookupDataResourceNBN(@Path("dataResourceUid") String dataResourceUid);
}
