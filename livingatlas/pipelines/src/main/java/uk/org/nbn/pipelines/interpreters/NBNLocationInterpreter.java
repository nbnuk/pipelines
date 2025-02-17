package uk.org.nbn.pipelines.interpreters;

import static org.gbif.api.vocabulary.OccurrenceIssue.COORDINATE_UNCERTAINTY_METERS_INVALID;
import static org.gbif.pipelines.core.utils.ModelUtils.addIssue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractNullAwareValue;

import au.org.ala.pipelines.vocabulary.ALAOccurrenceIssue;
import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.List;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.NumberParser;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.spark_project.guava.primitives.Doubles;
import org.spark_project.guava.primitives.Ints;
import uk.org.nbn.term.OSGridTerm;
import uk.org.nbn.util.GridUtil;
import uk.org.nbn.util.OSGridHelpers;
import uk.org.nbn.util.ScalaToJavaUtil;

public class NBNLocationInterpreter {
  public static void interpretCoordinateUncertaintyInMetersFromPrecisionFormat(
      ExtendedRecord er, LocationRecord lr) {

    String precisionValue = extractNullAwareValue(er, DwcTerm.coordinatePrecision);

    // check to see if the uncertainty has incorrectly been put in the precision
    // another way to misuse coordinatePrecision
    // why doesn't this check if uncertainty has already been set?
    if (!Strings.isNullOrEmpty(precisionValue)) {

      if (precisionValue.endsWith("km") || precisionValue.endsWith("m")) {
        Double precision = null;
        if (precisionValue.endsWith("km")) {
          Double precisionResult =
              Doubles.tryParse(
                  precisionValue.substring(0, precisionValue.length() - "km".length()));

          if (precisionResult != null && precisionResult > 0) {
            // multiply result to meters
            precision = precisionResult * 1000;
          }
          ;

        } else {
          precision =
              NumberParser.parseDouble(
                  precisionValue.substring(0, precisionValue.length() - "m".length()));
        }

        if (precision != null && precision > 1) {
          lr.setCoordinateUncertaintyInMeters(precision);

          // todo - not sure where comments go now
          String comment =
              "Supplied precision, " + precisionValue + ", is assumed to be uncertainty in metres";
          addIssue(lr, ALAOccurrenceIssue.UNCERTAINTY_IN_PRECISION.name());

          // Presumably this is removed as it is implied by the above issue
          removeIssue(lr, OccurrenceIssue.COORDINATE_PRECISION_INVALID.name());
        }
      }
    }
  }

  public static void interpretCoordinateUncertaintyInMeters(ExtendedRecord er, LocationRecord lr) {

    String gridReferenceValue = extractNullAwareValue(er, OSGridTerm.gridReference);
    String gridSizeInMetersValue = extractNullAwareValue(er, OSGridTerm.gridSizeInMeters);

    if (Strings.isNullOrEmpty(gridReferenceValue) && Strings.isNullOrEmpty(gridSizeInMetersValue)) {
      return;
    }

    String decimalLatitudeValue = extractNullAwareValue(er, DwcTerm.decimalLatitude);
    String decimalLongitudeValue = extractNullAwareValue(er, DwcTerm.decimalLongitude);
    String coordinateUncertaintyValue =
        extractNullAwareValue(er, DwcTerm.coordinateUncertaintyInMeters);

    boolean hasSuppliedLatLon =
        !Strings.isNullOrEmpty(decimalLatitudeValue)
            && !Strings.isNullOrEmpty(decimalLongitudeValue);

    // this was combined from checkUncertainty and possiblyRecalculateUncertainty
    // we know we have either a grid ref or grid size so we can compute uncertainty so...
    // set uncertainty if:
    // supplied without either lat/lon or uncertainty
    // we have a grid ref and the lat lon is centroid of the grid

    if (!hasSuppliedLatLon
        || Strings.isNullOrEmpty(coordinateUncertaintyValue)
        || (!Strings.isNullOrEmpty(gridReferenceValue)
            && GridUtil.isCentroid(
                Double.valueOf(decimalLongitudeValue),
                Double.valueOf(decimalLatitudeValue),
                gridReferenceValue))) {

      setCoordinateUncertaintyFromOSGrid(er, lr);
    }
  }

  private static void setCoordinateUncertaintyFromOSGrid(ExtendedRecord er, LocationRecord lr) {
    String gridReferenceValue = extractNullAwareValue(er, OSGridTerm.gridReference);
    String gridSizeInMetersValue = extractNullAwareValue(er, OSGridTerm.gridSizeInMeters);

    // todo - should we flag if these fail?  Internally this logs and error but this is not going to
    // be very helpful

    Integer gridSizeInMeters = null;

    if (!Strings.isNullOrEmpty(gridReferenceValue)) {
      gridSizeInMeters =
          ScalaToJavaUtil.scalaOptionToJavaInteger(
              GridUtil.getGridSizeInMeters(gridReferenceValue));
    } else {
      gridSizeInMeters = Ints.tryParse(gridSizeInMetersValue);
    }

    if (gridSizeInMeters != null) {
      double cornerDistFromCentre = OSGridHelpers.GridSizeToGridUncertainty(gridSizeInMeters);
      lr.setCoordinateUncertaintyInMeters(cornerDistFromCentre);
      removeIssue(lr, COORDINATE_UNCERTAINTY_METERS_INVALID.name());
    }
  }

  private static void removeIssue(LocationRecord lr, String issueName) {
    List<String> locationIssues = new ArrayList<>(lr.getIssues().getIssueList());
    locationIssues.remove(issueName);
    lr.getIssues().setIssueList(locationIssues);
  }
}
