package uk.org.nbn.pipelines.transforms;

import static au.org.ala.pipelines.transforms.IndexValues.PIPELINES_GEODETIC_DATUM;
import static org.gbif.pipelines.core.utils.ModelUtils.extractNullAwareValue;
import static uk.org.nbn.util.NBNModelUtils.*;

import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.core.functions.SerializableConsumer;
import org.gbif.pipelines.core.parsers.common.ParsedField;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transforms.converters.OccurrenceJsonTransform;
import uk.org.nbn.parser.OSGridParser;
import uk.org.nbn.pipelines.vocabulary.NBNOccurrenceIssue;
import uk.org.nbn.term.OSGridTerm;

/** Transform to augment the core location terms from osGrid extension terms */
@Slf4j
public class OSGridExtensionTransform extends DoFn<ExtendedRecord, ExtendedRecord> {

  private static final String OSGRID_RECORD_COUNTER = "oSGridExtensionCount";

  private final Counter counter =
      Metrics.counter(OccurrenceJsonTransform.class, OSGRID_RECORD_COUNTER);

  private SerializableConsumer<String> counterFn = v -> counter.inc();

  public static SingleOutput<ExtendedRecord, ExtendedRecord> create() {
    return ParDo.of(new OSGridExtensionTransform());
  }

  public void setCounterFn(SerializableConsumer<String> counterFn) {
    this.counterFn = counterFn;
  }

  @ProcessElement
  public void processElement(@Element ExtendedRecord er, OutputReceiver<ExtendedRecord> out) {
    convert(er, out::output);
  }

  public void convert(ExtendedRecord er, Consumer<ExtendedRecord> resultConsumer) {
    resultConsumer.accept(process(er));
  }

  public ExtendedRecord process(ExtendedRecord er) {
    String gridReferenceValue = extractNullAwareValue(er, OSGridTerm.gridReference);
    String gridSizeInMetersValue = extractNullAwareValue(er, OSGridTerm.gridSizeInMeters);

    if (Strings.isNullOrEmpty(gridReferenceValue) && Strings.isNullOrEmpty(gridSizeInMetersValue)) {
      return er;
    }

    ExtendedRecord alteredEr = ExtendedRecord.newBuilder(er).build();
    List<String> issues = new ArrayList<>();

    String decimalLatitudeValue = extractNullAwareValue(er, DwcTerm.decimalLatitude);
    String decimalLongitudeValue = extractNullAwareValue(er, DwcTerm.decimalLongitude);

    boolean hasSuppliedLatLon =
        !Strings.isNullOrEmpty(decimalLatitudeValue)
            && !Strings.isNullOrEmpty(decimalLongitudeValue);


    if(!Strings.isNullOrEmpty(gridReferenceValue)) {
      ParsedField<LatLng> result = OSGridParser.parseCoords(er);
      issues.addAll(result.getIssues());

      if (result.isSuccessful() && !hasSuppliedLatLon) {
        setLatLonFromOSGridParseResult(result, alteredEr);
      }
    }

    // put the issues in the extension so that we can retrieve and apply them in OSGridTransform
    setTermValue(alteredEr, OSGridTerm.issues, getStringFromList(issues));

    counter.inc();
    return alteredEr;
  }

  private void setLatLonFromOSGridParseResult(ParsedField<LatLng> result, ExtendedRecord alteredEr) {
    if (result.isSuccessful()) {
      alteredEr
          .getCoreTerms()
          .put(
              DwcTerm.decimalLatitude.qualifiedName(), result.getResult().getLatitude().toString());
      alteredEr
          .getCoreTerms()
          .put(
              DwcTerm.decimalLongitude.qualifiedName(),
              result.getResult().getLongitude().toString());
      // grid util projects all coordinates to WGS84
      alteredEr.getCoreTerms().put(DwcTerm.geodeticDatum.qualifiedName(), PIPELINES_GEODETIC_DATUM);
    }
  }
}
