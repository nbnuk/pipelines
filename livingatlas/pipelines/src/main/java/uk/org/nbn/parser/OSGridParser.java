package uk.org.nbn.parser;

import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.common.parsers.NumberParser;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.core.parsers.common.ParsedField;
import org.gbif.pipelines.core.parsers.location.parser.CoordinateParseUtils;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.mutable.ListBuffer;
import uk.org.nbn.util.GISPoint;
import uk.org.nbn.util.GridUtil;
import uk.org.nbn.vocabulary.NBNOccurrenceIssue;

import java.util.HashSet;
import java.util.List;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class OSGridParser {

    public static ParsedField<LatLng> parseGridReference(final String gridReference) {
        if (Strings.isNullOrEmpty(gridReference)) {
            return ParsedField.fail();
        }

        Option<GISPoint> result = GridUtil.processGridReference(gridReference);

        if(result.isEmpty())
        {
            return ParsedField.fail();
        }

        GISPoint gisPoint = result.get();

        // Use the core CoordinateParseUtils to parse the lat lon string and standardised them in the same way as if lat lon supplied
        ParsedField<LatLng> ret = CoordinateParseUtils.parseLatLng(gisPoint.latitude(), gisPoint.longitude());
        ret.getIssues().add(NBNOccurrenceIssue.DECIMAL_LAT_LONG_CALCULATED_FROM_GRID_REF.name());
        return  ret;
    }

    public static ParsedField<LatLng> parseEastingAndNorthing(final String verbatimSRS, final String easting, final String northing, final String zone) {
        if (
                Strings.isNullOrEmpty(easting) ||
                Strings.isNullOrEmpty(northing) ||
                Strings.isNullOrEmpty(zone))
        {
            return ParsedField.fail();
        }

        ListBuffer<String> scalaIssues = new ListBuffer<>();
        Option<GISPoint> gisPointResult = GridUtil.processNorthingEastingZone(verbatimSRS, easting, northing, zone, scalaIssues);

        List<String> issues = JavaConverters.seqAsJavaListConverter(scalaIssues).asJava();

        if (gisPointResult.isEmpty())
        {
            return  ParsedField.fail(new HashSet<>(issues));
        }

        GISPoint gisPoint = gisPointResult.get();

        // Use the core CoordinateParseUtils to parse the lat lon string and standardised them in the same way as if lat lon supplied
        ParsedField<LatLng> ret = CoordinateParseUtils.parseLatLng(gisPoint.latitude(), gisPoint.longitude());
        ret.getIssues().addAll(issues);
        return ret;
    }
}
