package uk.org.nbn.parser;

import static org.gbif.pipelines.core.utils.ModelUtils.extractNullAwareValue;

import au.org.ala.pipelines.vocabulary.ALAOccurrenceIssue;
import com.google.common.base.Strings;
import java.util.*;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.core.parsers.common.ParsedField;
import org.gbif.pipelines.core.parsers.location.parser.CoordinateParseUtils;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import scala.Option;
import uk.org.nbn.pipelines.vocabulary.NBNOccurrenceIssue;
import uk.org.nbn.term.OSGridTerm;
import uk.org.nbn.util.GISPoint;
import uk.org.nbn.util.GridUtil;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class OSGridParser {

  // parses OSGrid extension gridRefernce field
  private static final Function<ExtendedRecord, ParsedField<LatLng>> GRID_REFERENCE_FN =
      (er -> parseGridReference(extractNullAwareValue(er, OSGridTerm.gridReference)));

  private static final List<Function<ExtendedRecord, ParsedField<LatLng>>> PARSING_FUNCTIONS =
      Arrays.asList(OSGridParser.GRID_REFERENCE_FN);

  public static ParsedField<LatLng> parseCoords(ExtendedRecord extendedRecord) {
    Set<String> issues = new TreeSet<>();
    for (Function<ExtendedRecord, ParsedField<LatLng>> parsingFunction : PARSING_FUNCTIONS) {
      ParsedField<LatLng> result = parsingFunction.apply(extendedRecord);
      if (result.isSuccessful()) {
        return result;
      }
      issues.addAll(result.getIssues());
    }
    issues.add(ALAOccurrenceIssue.LOCATION_NOT_SUPPLIED.name());
    return ParsedField.fail(issues);
  }

  private static ParsedField<LatLng> parseGridReference(final String gridReference) {
    if (Strings.isNullOrEmpty(gridReference)) {
      return ParsedField.fail();
    }

    Option<GISPoint> result = GridUtil.processGridReference(gridReference);

    if (result.isEmpty()) {
      return ParsedField.fail(NBNOccurrenceIssue.GRID_REF_INVALID.name());
    }

    GISPoint gisPoint = result.get();

    // Use the core CoordinateParseUtils to parse the lat lon string and standardised them in the
    // same way as if lat lon supplied
    ParsedField<LatLng> ret =
        CoordinateParseUtils.parseLatLng(gisPoint.latitude(), gisPoint.longitude());
    ret.getIssues().add(NBNOccurrenceIssue.DECIMAL_LAT_LONG_CALCULATED_FROM_GRID_REF.name());
    return ret;
  }
}
