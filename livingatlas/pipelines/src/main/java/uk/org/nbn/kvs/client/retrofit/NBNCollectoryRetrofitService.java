package uk.org.nbn.kvs.client.retrofit;

import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Path;
import uk.org.nbn.kvs.client.DataResourceNBN;

/** Collectory retrofit web service interface */
public interface NBNCollectoryRetrofitService {

  @GET("accessControl/dataResourceNbn/{dataResourceUid}")
  Call<DataResourceNBN> lookupDataResourceNBN(@Path("dataResourceUid") String dataResourceUid);
}
