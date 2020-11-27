package au.org.ala.kvs;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import lombok.Data;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.config.model.WsConfig;

/** Living Atlas configuration extensions */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ALAPipelinesConfig implements Serializable {

  PipelinesConfig gbifConfig;
  GeocodeShpConfig geocodeConfig;
  LocationInfoConfig locationInfoConfig;
  // ALA specific
  private WsConfig collectory;
  private WsConfig alaNameMatch;
  private WsConfig sds;
  private WsConfig lists;
  private WsConfig imageService;

  public ALAPipelinesConfig() {
    gbifConfig = new PipelinesConfig();
    locationInfoConfig = new LocationInfoConfig();
    collectory = new WsConfig();
    alaNameMatch = new WsConfig();
    lists = new WsConfig();
    imageService = new WsConfig();
  }
}
