package uk.org.nbn.transforms;

import au.org.ala.kvs.ALAPipelinesConfig;
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
import org.gbif.pipelines.io.avro.GridReferenceRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.transforms.Transform;
import uk.org.nbn.pipelines.interpreters.OSGridInterpreter;

import java.util.Optional;

import static uk.org.nbn.pipelines.common.NBNRecordTypes.GRID_REFERENCE;

@Slf4j
public class OSGridExtensionTransform extends Transform<KV<String,CoGbkResult>, GridReferenceRecord> {

  private final ALAPipelinesConfig alaConfig;
  @NonNull
  private final TupleTag<ExtendedRecord> erTag;
  @NonNull private final TupleTag<LocationRecord> lrTag;

  @Builder(buildMethodName = "create")
  private OSGridExtensionTransform(
          ALAPipelinesConfig alaConfig, @NonNull TupleTag<ExtendedRecord> erTag, @NonNull TupleTag<LocationRecord> lrTag
  ) {

    super(
            GridReferenceRecord.class,
            GRID_REFERENCE,
            OSGridExtensionTransform.class.getName(),
            "gridReferenceRecordCount");

    this.alaConfig = alaConfig;
    this.erTag = erTag;
    this.lrTag = lrTag;
  }

  /** Beam @Setup initializes resources */
  @SneakyThrows
  @Setup
  public void setup() {

  }

  /** Beam @Setup can be applied only to void method */
  public OSGridExtensionTransform init() {
    setup();
    return this;
  }

  /** Beam @Teardown closes initialized resources */
  @Teardown
  public void tearDown() {

  }

  public OSGridExtensionTransform counterFn(SerializableConsumer<String> counterFn) {
    setCounterFn(counterFn);
    return this;
  }

  @Override
  public Optional<GridReferenceRecord> convert(KV<String, CoGbkResult> source) {

    CoGbkResult v = source.getValue();
    String id = source.getKey();

    if (v == null) {
      return Optional.empty();
    }

    LocationRecord locationRecord = lrTag == null ? null : v.getOnly(lrTag, null);
    ExtendedRecord extendedRecord = erTag == null ? null : v.getOnly(erTag, null);

    //We need to:
    //Set lat lon from grid when grid provided
    //Run location transform
    //Set grid from lat lon

    GridReferenceRecord gridReferenceRecord =
            GridReferenceRecord.newBuilder()
                    .setId(source.getKey())
                    .build();

    Optional<GridReferenceRecord> result =
            Interpretation.from(new Tuple<>(extendedRecord, locationRecord))
                    .to(gridReferenceRecord)
                    .when(er -> !er.v1().getCoreTerms().isEmpty())
                    //This populate grid sizes for those supplied with gridreference or gridsizeinmeters
                    .via(OSGridInterpreter::addGridSize)
                    .via(OSGridInterpreter::possiblyRecalculateCoordinateUncertainty)
                    .via(OSGridInterpreter::setGridRefFromCoordinates)
                    //This populates grids sizes for those supplied with a lat lot and have had gridreference computed
                    .via(OSGridInterpreter::addGridSize)
                    .via(OSGridInterpreter::processGridWKT)
                    .get();

    result.ifPresent(r -> this.incCounter());

    return result;
  }


  public MapElements<GridReferenceRecord, KV<String, GridReferenceRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, GridReferenceRecord>>() {})
            .via((GridReferenceRecord lr) -> KV.of(lr.getId(), lr));
  }
}
