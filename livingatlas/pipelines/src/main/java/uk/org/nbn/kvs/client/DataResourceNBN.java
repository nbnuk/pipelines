package uk.org.nbn.kvs.client;

import au.org.ala.kvs.client.ALACollectoryMetadata;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.Builder;
import lombok.ToString;
import lombok.Value;

/**
 * An ALA Collectory Data Resource response object. This maps on to a data resource response e.g.
 * https://collections.ala.org.au/ws/dataResource/dr376
 */
@JsonDeserialize(builder = DataResourceNBN.DataResourceNBNBuilder.class)
@Value
@Builder
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataResourceNBN {

  ALACollectoryMetadata dataResource;
  Integer publicResolution;
  Integer publicResolutionToBeApplied;
  Boolean needToReload;
//
//  Date dateCreated
//  Date lastUpdated




  @JsonPOJOBuilder(withPrefix = "")
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class DataResourceNBNBuilder {}

  public static final DataResourceNBN EMPTY = null;
}
