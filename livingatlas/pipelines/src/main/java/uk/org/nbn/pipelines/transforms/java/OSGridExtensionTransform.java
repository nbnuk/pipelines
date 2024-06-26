package uk.org.nbn.pipelines.transforms.java;

import lombok.NoArgsConstructor;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Java level transformation for sampling event where occurrence records stored in extensions
 *
 * @see <a href="https://github.com/gbif/ipt/wiki/BestPracticesSamplingEventData>Sampling event</a>
 */
@NoArgsConstructor(staticName = "create")
public class OSGridExtensionTransform {

  private final uk.org.nbn.pipelines.transforms.OSGridExtensionTransform transform =
      new uk.org.nbn.pipelines.transforms.OSGridExtensionTransform();

  public OSGridExtensionTransform counterFn(SerializableConsumer<String> counterFn) {
    transform.setCounterFn(counterFn);
    return this;
  }

  public Map<String, ExtendedRecord> transform(Map<String, ExtendedRecord> erMap) {

    Map<String, ExtendedRecord> result = new HashMap<>();

    Consumer<ExtendedRecord> consumer =
        r -> {
          if (r != null && r.getId() != null && !r.getId().isEmpty()) {
            result.put(r.getId(), r);
          }
        };

    erMap.values().forEach(er -> transform.convert(er, consumer));

    return result;
  }
}
