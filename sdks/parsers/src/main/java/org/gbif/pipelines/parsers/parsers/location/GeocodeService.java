package org.gbif.pipelines.parsers.parsers.location;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.Serializable;

import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.parsers.parsers.location.cache.GeocodeBitmapCache;
import org.gbif.rest.client.geocode.GeocodeResponse;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GeocodeService implements Serializable {

  private static final long serialVersionUID = -2090636199984570712L;

  private final KeyValueStore<LatLng, GeocodeResponse> kvStore;
  private final GeocodeBitmapCache bitmapCache;

  private GeocodeService(@NonNull KeyValueStore<LatLng, GeocodeResponse> kvStore, BufferedImage image) {
    this.kvStore = kvStore;
    this.bitmapCache = image == null ? null : GeocodeBitmapCache.create(image, kvStore::get);
  }

  public static GeocodeService create(KeyValueStore<LatLng, GeocodeResponse> kvStore, BufferedImage image) {
    return new GeocodeService(kvStore, image);
  }

  public static GeocodeService create(KeyValueStore<LatLng, GeocodeResponse> kvStore) {
    return new GeocodeService(kvStore, null);
  }

  /** Simple get candidates by point. */
  public GeocodeResponse get(LatLng latLng) {
    GeocodeResponse locations = null;

    // Check the image map for a sure location.
    if (bitmapCache != null) {
      locations = bitmapCache.getFromBitmap(latLng);
    }

    // If that doesn't help, use the database.
    if (locations == null) {
      locations = kvStore.get(latLng);
    }

    return locations;
  }

  public void close() {
    if (kvStore != null) {
      try {
        kvStore.close();
      } catch (IOException ex) {
        log.error("Error closing KVStore", ex);
      }
    }
  }

}
