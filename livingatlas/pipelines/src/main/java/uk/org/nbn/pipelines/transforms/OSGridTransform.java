package uk.org.nbn.pipelines.transforms;

import static uk.org.nbn.pipelines.common.NBNRecordTypes.OS_GRID;

import au.org.ala.kvs.ALAPipelinesConfig;
import java.time.Instant;
import java.util.Optional;
import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.elasticsearch.common.collect.Tuple;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.OSGridRecord;
import org.gbif.pipelines.transforms.Transform;
import uk.org.nbn.pipelines.interpreters.OSGridInterpreter;

@Slf4j
public class OSGridTransform extends Transform<KV<String, CoGbkResult>, OSGridRecord> {

  private final ALAPipelinesConfig alaConfig;
  @NonNull private final TupleTag<ExtendedRecord> erTag;
  @NonNull private final TupleTag<LocationRecord> lrTag;

  @Builder(buildMethodName = "create")
  private OSGridTransform(
      ALAPipelinesConfig alaConfig,
      TupleTag<ExtendedRecord> erTag,
      TupleTag<LocationRecord> lrTag) {

    super(
        OSGridRecord.class,
        OS_GRID,
        uk.org.nbn.pipelines.transforms.OSGridTransform.class.getName(),
        "osGridRecordCount");

    this.alaConfig = alaConfig;
    this.erTag = erTag;
    this.lrTag = lrTag;
  }

  /** Beam @Setup initializes resources */
  @SneakyThrows
  @Setup
  public void setup() {}

  /** Beam @Setup can be applied only to void method */
  public uk.org.nbn.pipelines.transforms.OSGridTransform init() {
    setup();
    return this;
  }

  /** Beam @Teardown closes initialized resources */
  @Teardown
  public void tearDown() {}

  public uk.org.nbn.pipelines.transforms.OSGridTransform counterFn(
      SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  @Override
  public Optional<OSGridRecord> convert(KV<String, CoGbkResult> source) {

    CoGbkResult v = source.getValue();
    String id = source.getKey();

    if (v == null) {
      return Optional.empty();
    }

    LocationRecord locationRecord = lrTag == null ? null : v.getOnly(lrTag, null);
    ExtendedRecord extendedRecord = erTag == null ? null : v.getOnly(erTag, null);

    // We need to:
    // Set lat lon from grid when grid provided
    // Run location transform
    // Set grid from lat lon

    return processElement(extendedRecord, locationRecord);
  }

  public MapElements<OSGridRecord, KV<String, OSGridRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, OSGridRecord>>() {})
        .via((OSGridRecord lr) -> KV.of(lr.getId(), lr));
  }

  public Optional<OSGridRecord> processElement(ExtendedRecord extendedRecord, LocationRecord locationRecord) {

    OSGridRecord osGridRecord =
            OSGridRecord.newBuilder()
                    .setId(extendedRecord.getId())
                    .setCreated(Instant.now().toEpochMilli())
                    .build();


    Optional<OSGridRecord> result =
            Interpretation.from(new Tuple<>(extendedRecord, locationRecord))
                    .to(osGridRecord)
                    .when(er -> !er.v1().getCoreTerms().isEmpty())
                    .via(OSGridInterpreter::applyIssues)
                    // This populate grid sizes for those supplied with gridreference or gridsizeinmeters
                    .via(OSGridInterpreter::addGridSize)
                    // .via(OSGridInterpreter::possiblyRecalculateCoordinateUncertainty)
                    .via(OSGridInterpreter::validateSuppliedGridReferenceAndLatLon)
                    .via(OSGridInterpreter::setGridRefFromCoordinates)
                    // This populates grids sizes for those supplied with a lat lot and have had
                    // gridreference computed
                    .via(OSGridInterpreter::addGridSize)
                    .via(OSGridInterpreter::processGridWKT)
                    .get();

    result.ifPresent(r -> this.incCounter());

    return result;
  }
}
