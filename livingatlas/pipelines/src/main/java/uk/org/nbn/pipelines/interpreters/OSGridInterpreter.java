package uk.org.nbn.pipelines.interpreters;

import static org.gbif.api.vocabulary.OccurrenceIssue.COORDINATE_UNCERTAINTY_METERS_INVALID;
import static org.gbif.pipelines.core.utils.ModelUtils.*;
import static uk.org.nbn.util.NBNModelUtils.getListFromString;
import static uk.org.nbn.util.ScalaToJavaUtil.scalaOptionToString;

import com.google.common.base.Strings;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.elasticsearch.common.collect.Tuple;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.geospatial.MeterRangeParser;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.OSGridRecord;
import uk.org.nbn.term.OSGridTerm;
import uk.org.nbn.util.GISPoint;
import uk.org.nbn.util.GridRef;
import uk.org.nbn.util.GridUtil;
import uk.org.nbn.pipelines.vocabulary.NBNOccurrenceIssue;

public class OSGridInterpreter {

  //    public static void interpretCoordinateUncertaintyInMetersFromGridSize(ExtendedRecord er,
  // LocationRecord lr) {
  //        String uncertaintyValue = extractNullAwareValue(er,
  // DwcTerm.coordinateUncertaintyInMeters);
  //        String gridSizeInMetersValue = extractNullAwareExtensionTerm(er,
  // OSGridTerm.gridSizeInMeters);
  //
  //        if((Strings.isNullOrEmpty(uncertaintyValue) ||
  //                lr.getCoordinateUncertaintyInMeters() == null ||
  //                lr.getCoordinateUncertaintyInMeters() < 0.000001) &&
  //                !Strings.isNullOrEmpty(gridSizeInMetersValue)
  //        ) {
  //            Double gridSizeInMeters = Doubles.tryParse(gridSizeInMetersValue);
  //
  //            // old version didn't have null check
  //            if(gridSizeInMeters != null) {
  //                double cornerDistFromCentre = roundToDecimalPlaces( gridSizeInMeters /
  // Math.sqrt(2.0),1);
  //                lr.setCoordinateUncertaintyInMeters(cornerDistFromCentre);
  //            }
  //        }
  //    }

  /**
   * This method is currently/historically called twice Expect OSGrid records to be updated the
   * first time but not second Expect non-OSGrid records to be updated the second time but not the
   * first
   *
   * <p>Expect to be set from gridReference when supplied Expect to be set from gridSizeInMeteres
   * when Easting and Northing supplied Expect to be set from computed gridReference when lan lon
   * supplied with coordinateUncertainty Expect not to be set when lan lon supplie without
   * coordinateUncertainty
   *
   * @param source
   * @param osGridRecord
   */
  public static void addGridSize(
      Tuple<ExtendedRecord, LocationRecord> source, OSGridRecord osGridRecord) {

    ExtendedRecord extendedRecord = source.v1();

    String gridReference =
            extractNullAwareValue(extendedRecord, OSGridTerm.gridReference);
    String gridSizeInMeters =
            extractNullAwareValue(extendedRecord, OSGridTerm.gridSizeInMeters);

    if (osGridRecord.getGridSizeInMeters() == null) {
      if (!Strings.isNullOrEmpty(osGridRecord.getGridReference())) {
        // I think this will only ever be hit for records supplied with lat lon on the second call
        // and the other two only on the first call
        Integer computedGridSizeInMeters =
            (Integer) GridUtil.getGridSizeInMeters(osGridRecord.getGridReference()).getOrElse(null);
        osGridRecord.setGridSizeInMeters(computedGridSizeInMeters);

      } else if (!Strings.isNullOrEmpty(gridReference)) {
        Integer computedGridSizeInMeters =
            (Integer) GridUtil.getGridSizeInMeters(gridReference).getOrElse(null);
        osGridRecord.setGridSizeInMeters(computedGridSizeInMeters);

      } else if (!Strings.isNullOrEmpty(gridSizeInMeters)) {
        Integer computedGridSizeInMeters = Ints.tryParse(gridSizeInMeters);
        osGridRecord.setGridSizeInMeters(computedGridSizeInMeters);
      }

      // As GridSizeInMeters is Integer it cannot be empty so need to set null here anymore
    }
  }

  /**
   * Extract issue from OSGrid extension term and apply to model
   *
   * @param source
   * @param osGridRecord
   */
  public static void applyIssues(
      Tuple<ExtendedRecord, LocationRecord> source, OSGridRecord osGridRecord) {

    ExtendedRecord extendedRecord = source.v1();
    String gridReferenceIssues =
            extractNullAwareValue(extendedRecord, OSGridTerm.issues);

    if (!Strings.isNullOrEmpty(gridReferenceIssues)) {
      getListFromString(gridReferenceIssues).stream()
          .map(
              issue ->
                  Arrays.stream(NBNOccurrenceIssue.values())
                      .filter(i -> i.name().equals(issue))
                      .findFirst())
          .filter(issue -> issue.isPresent())
          .forEach(issue -> addIssue(osGridRecord, issue.get().name()));
    }
  }

  /**
   * Where both grid reference and lat lon supplied, adds assertion if lat lon is not centroid of
   * grid square
   *
   * @param source
   * @param osGridRecord
   */
  public static void validateSuppliedGridReferenceAndLatLon(
      Tuple<ExtendedRecord, LocationRecord> source, OSGridRecord osGridRecord) {

    ExtendedRecord extendedRecord = source.v1();

    String rawGridReference =
            extractNullAwareValue(extendedRecord, OSGridTerm.gridReference);

    if (Strings.isNullOrEmpty(rawGridReference)) {
      return;
    }

    String decimalLatitudeValue = extractNullAwareValue(extendedRecord, DwcTerm.decimalLatitude);
    String decimalLongitudeValue = extractNullAwareValue(extendedRecord, DwcTerm.decimalLongitude);

    if (suppliedWithLatLon(extendedRecord, osGridRecord)
        && !GridUtil.isCentroid(
            Double.valueOf(decimalLongitudeValue),
            Double.valueOf(decimalLatitudeValue),
            rawGridReference)) {
      addIssue(osGridRecord, NBNOccurrenceIssue.COORDINATES_NOT_CENTRE_OF_GRID.name());
    }
  }
  
  /**
   * This sets the grid reference from lat lon for all records OSGrid records - OSGrid > lat lon >
   * OSGrid Non OSGrid records - lat lon > OSGrid
   *
   * <p>Expect OSGrid records to be based on lat lon and gridSizeInMeters set in addGridSize
   *
   * @param source
   * @param osGridRecord
   */
  public static void setGridRefFromCoordinates(
      Tuple<ExtendedRecord, LocationRecord> source, OSGridRecord osGridRecord) {

    ExtendedRecord extendedRecord = source.v1();
    LocationRecord locationRecord = source.v2();
    Double coordinateUncertainty = locationRecord.getCoordinateUncertaintyInMeters();

    if (locationRecord.getHasCoordinate()
        && Strings.isNullOrEmpty(osGridRecord.getGridReference())
        && coordinateUncertainty != null
        && coordinateUncertainty > 0) {
      List<String> gbList =
          Arrays.asList(
              "Wales", "Scotland", "England", "Isle of Man"); // OSGB-grid countries hard-coded
      List<String> niList = Arrays.asList("Northern Ireland"); // Irish grid

      String gridCalc;
      String gridToUse = "OSGB";

      if (gbList.contains(locationRecord.getStateProvince())) {
        gridToUse = "OSGB";
      } else if (niList.contains(locationRecord.getStateProvince())
          || isIrishLatLon(
              locationRecord.getDecimalLongitude(), locationRecord.getDecimalLatitude())) {
        gridToUse = "Irish";
      }

      // this will be the case for records supplied with OSGrid details whereas one supplied with
      // lat lon wont get their grid size set until we've computed and grid reference
      if (osGridRecord.getGridSizeInMeters() != null) {
        gridCalc =
            scalaOptionToString(
                GridUtil.latLonToOsGrid(
                    locationRecord.getDecimalLatitude(),
                    locationRecord.getDecimalLongitude(),
                    coordinateUncertainty,
                    "WGS84",
                    gridToUse,
                    osGridRecord.getGridSizeInMeters()));
      } else {
        // todo we could cobmine these two calls as does seem to allow optional argument to be
        // ommited so just supplying default
        gridCalc =
            scalaOptionToString(
                GridUtil.latLonToOsGrid(
                    locationRecord.getDecimalLatitude(),
                    locationRecord.getDecimalLongitude(),
                    coordinateUncertainty,
                    "WGS84",
                    gridToUse,
                    -1));
      }

      if (!Strings.isNullOrEmpty(gridCalc)) {
        osGridRecord.setGridReference(gridCalc);

        // todo - what about northing and easting? this is how it's alway been though
        if (!suppliedWithGridReference(extendedRecord)) {
          addIssue(osGridRecord, NBNOccurrenceIssue.GRID_REF_CALCULATED_FROM_LAT_LONG.name());
        }
      }
    }
  }

  private static boolean isIrishLatLon(double longitude, double latitude) {
    return longitude < -5.0 && latitude < 57.0 && latitude > 48.0;
  }

  private static boolean suppliedWithGridReference(ExtendedRecord extendedRecord) {
    return !Strings.isNullOrEmpty(
            extractNullAwareValue(extendedRecord, OSGridTerm.gridReference));
  }

  private static boolean suppliedWithLatLon(
      ExtendedRecord extendedRecord, OSGridRecord osGridRecord) {

    boolean rawLatLonWasComputedFromOSGrid =
        osGridRecord
                .getIssues()
                .getIssueList()
                .contains(NBNOccurrenceIssue.DECIMAL_LAT_LONG_CALCULATED_FROM_GRID_REF.name());

    return !rawLatLonWasComputedFromOSGrid
        && !Strings.isNullOrEmpty(extractNullAwareValue(extendedRecord, DwcTerm.decimalLatitude))
        && !Strings.isNullOrEmpty(extractNullAwareValue(extendedRecord, DwcTerm.decimalLongitude));
  }

  public static void processGridWKT(
      Tuple<ExtendedRecord, LocationRecord> source, OSGridRecord osGridRecord) {

    ExtendedRecord extendedRecord = source.v1();

    if (suppliedWithLatLon(extendedRecord, osGridRecord)) {
      return;
    }

    // surely this will alway be empty in pipelines land
    if (Strings.isNullOrEmpty(osGridRecord.getGridReferenceWKT())
        && suppliedWithGridReference(extendedRecord)) {

      boolean computed = false;

      String gridReference =
              extractNullAwareValue(extendedRecord, OSGridTerm.gridReference);

      // should this not be empty checked as well?
      if (osGridRecord.getGridReference() != null) {
        osGridRecord.setGridReferenceWKT(GridUtil.getGridWKT(osGridRecord.getGridReference()));
        computed = true;
      }
      // should this not be empty checked as well? and if so surely this is always true as it was
      // previously empty checked
      // and if so what's the point of computed?
      else if (gridReference != null) {
        osGridRecord.setGridReferenceWKT(GridUtil.getGridWKT(gridReference));
        computed = true;
      }

      // todo - agreed to remove however this should be handled in sds or access controls
      //            if (computed) {
      //                if (processed.occurrence.informationWithheld == null)
      //                    processed.occurrence.informationWithheld = ""
      //                else
      //                    processed.occurrence.informationWithheld =
      // processed.occurrence.informationWithheld + " "
      //
      //                processed.occurrence.informationWithheld =
      // processed.occurrence.informationWithheld +
      // GridUtil.getGridAsTextWithAnnotation(raw.location.gridReference)
      //                // note, we don't overwrite raw.occurrence.informationWithheld, as we might
      // prefer that untouched
      //            }
    }
  }

  public static void addEastingAndNorthing(Tuple<ExtendedRecord, LocationRecord> source, OSGridRecord osGridRecord) {
      //In biocache-store only records grid reference produces easting and northing.
      //todo -should we be using the processed value
      ExtendedRecord extendedRecord = source.v1();
      if(suppliedWithGridReference(extendedRecord)) {
        String rawGridReference =
                extractNullAwareValue(extendedRecord, OSGridTerm.gridReference);
        GISPoint gisPoint = GridUtil.processGridReference(rawGridReference).getOrElse(null);

        if(gisPoint != null) {
          osGridRecord.setEasting(Ints.tryParse(gisPoint.easting()));
          osGridRecord.setNorthing(Ints.tryParse(gisPoint.northing()));
        }
      }
  }

  public static void setCoreId(ExtendedRecord er, OSGridRecord e) {
    Optional.ofNullable(er.getCoreId()).ifPresent(e::setCoreId);
  }
}
