package uk.org.nbn.kvs.client;

import retrofit2.http.Path;

import java.io.Closeable;

/** An interface for the collectory web services */
public interface NBNCollectoryService extends Closeable {

  /** Retrieve the details of a data resource. */
  DataResourceNBN lookupDataResourceNBN(@Path("dataResourceUid") String dataResourceUid);


}
