package uk.org.nbn.pipelines.interpreters;

import com.google.common.base.Strings;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import org.elasticsearch.common.collect.Tuple;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.geospatial.MeterRangeParser;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.OSGridRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import uk.org.nbn.term.OSGridTerm;
import uk.org.nbn.util.GridUtil;
import uk.org.nbn.vocabulary.NBNOccurrenceIssue;

import java.util.Arrays;
import java.util.List;

import static org.gbif.api.vocabulary.OccurrenceIssue.COORDINATE_UNCERTAINTY_METERS_INVALID;

import static org.gbif.pipelines.core.utils.ModelUtils.*;
import static uk.org.nbn.util.NBNModelUtils.extractNullAwareExtensionTerm;
import static uk.org.nbn.util.ScalaToJavaUtil.scalaOptionToString;

public class OSGridInterpreter {

    public static void interpretCoordinateUncertaintyInMetersFromGridSize(ExtendedRecord er, LocationRecord lr) {
        String uncertaintyValue = extractNullAwareValue(er, DwcTerm.coordinateUncertaintyInMeters);
        String gridSizeInMetersValue = extractNullAwareExtensionTerm(er, OSGridTerm.gridSizeInMeters);

        if((Strings.isNullOrEmpty(uncertaintyValue) ||
                lr.getCoordinateUncertaintyInMeters() == null ||
                lr.getCoordinateUncertaintyInMeters() < 0.000001) &&
                !Strings.isNullOrEmpty(gridSizeInMetersValue)
        ) {
            Double gridSizeInMeters = Doubles.tryParse(gridSizeInMetersValue);

            // old version didn't have null check
            if(gridSizeInMeters != null) {
                double cornerDistFromCentre = roundToDecimalPlaces( gridSizeInMeters / Math.sqrt(2.0),1);
                lr.setCoordinateUncertaintyInMeters(cornerDistFromCentre);
            }
        }
    }



    public static void addGridSize(Tuple<ExtendedRecord,LocationRecord> source, OSGridRecord osGridRecord) {

        ExtendedRecord extendedRecord = source.v1();

        String gridReference = extractNullAwareExtensionTerm(extendedRecord, OSGridTerm.gridReference);
        String gridSizeInMeters = extractNullAwareExtensionTerm(extendedRecord, OSGridTerm.gridSizeInMeters);

        //if we didn't call this twice this could go
        if(osGridRecord.getGridSizeInMeters() == null)
        {
            if(!Strings.isNullOrEmpty(osGridRecord.getGridReference())) {
                //I think this will only ever be hit for records supplied with lat lon on the second call and the other two only on the first call
                Integer computedGridSizeInMeters = (Integer) GridUtil.getGridSizeInMeters(osGridRecord.getGridReference()).getOrElse(null);
                osGridRecord.setGridSizeInMeters(computedGridSizeInMeters);

            } else if (!Strings.isNullOrEmpty(gridReference)) {
                Integer computedGridSizeInMeters = (Integer) GridUtil.getGridSizeInMeters(gridReference).getOrElse(null);
                osGridRecord.setGridSizeInMeters(computedGridSizeInMeters);

            } else if (!Strings.isNullOrEmpty(gridSizeInMeters)) {
                Integer computedGridSizeInMeters = Ints.tryParse(gridSizeInMeters);
                osGridRecord.setGridSizeInMeters(computedGridSizeInMeters);

            }

            // As GridSizeInMeters is Integer it cannot be empty so need to set null here anymore
        }
    }

    public static void possiblyRecalculateCoordinateUncertainty(Tuple<ExtendedRecord,LocationRecord> source, OSGridRecord osGridRecord) {
        // comments from biocache-store
        // f grid and no lat/long
        //or if grid and lat/long, and lat/long is centroid
        //or if grid and lat/long and no coordinate uncertainty provided
        //then amend coordinate uncertainty to radius of circle through corners of grid

        ExtendedRecord extendedRecord = source.v1();

        String rawGridReference = extractNullAwareExtensionTerm(extendedRecord, OSGridTerm.gridReference);
        String rawGridSizeInMeters = extractNullAwareExtensionTerm(extendedRecord, OSGridTerm.gridSizeInMeters);

        if(Strings.isNullOrEmpty(rawGridReference)) {
            return;
        }

        String decimalLatitudeValue = extractNullAwareValue(extendedRecord, DwcTerm.decimalLatitude);
        String decimalLongitudeValue = extractNullAwareValue(extendedRecord, DwcTerm.decimalLongitude);
        String coordinateUncertaintyValue = extractNullAwareValue(extendedRecord, DwcTerm.coordinateUncertaintyInMeters);

        boolean hasRawLatLon = !Strings.isNullOrEmpty(decimalLatitudeValue) && !Strings.isNullOrEmpty(decimalLongitudeValue);
        boolean rawLatLonIsCentroidOfGridReference = hasRawLatLon && GridUtil.isCentroid(Double.valueOf(decimalLatitudeValue), Double.valueOf(decimalLongitudeValue), rawGridReference);

        // It feels odd to be checking for a raw uncertainty here as the location processor will have done parsing on this and tried to resolve from precission
        boolean hasRawLatLonButNoUncertaintySupplied = hasRawLatLon && Strings.isNullOrEmpty(coordinateUncertaintyValue);

        boolean recalcCoordUncertainty = !hasRawLatLon || rawLatLonIsCentroidOfGridReference || hasRawLatLonButNoUncertaintySupplied;

        if(hasRawLatLon && !rawLatLonIsCentroidOfGridReference)
        {
            addIssue(osGridRecord, NBNOccurrenceIssue.COORDINATES_NOT_CENTRE_OF_GRID.name());
        }

        if(recalcCoordUncertainty) {

            double cornerDistanceFromCentre = -1;

            if(osGridRecord.getGridSizeInMeters() != null) {
                cornerDistanceFromCentre = osGridRecord.getGridSizeInMeters() / Math.sqrt(2.0);
            } else if (!Strings.isNullOrEmpty(rawGridSizeInMeters)) {
                //todo this should never happen as the above should always be true if not then we have a problem?
                cornerDistanceFromCentre = Doubles.tryParse(rawGridSizeInMeters) / Math.sqrt(2.0);
            }

            if(cornerDistanceFromCentre >= 0) {
                //See LocationInterpreter for this handling
                ParseResult<Double> parseResult = MeterRangeParser.parseMeters(Double.toString(cornerDistanceFromCentre));
                Double result = parseResult.isSuccessful() ? Math.abs(cornerDistanceFromCentre) : null;

                if (result != null) {
                    //todo - make sure this is used in indexing if set
                    osGridRecord.setCoordinateUncertaintyInMeters(result);
                } else {
                    addIssue(osGridRecord, COORDINATE_UNCERTAINTY_METERS_INVALID);
                }
            }
        }
    }

    public static void setGridRefFromCoordinates(Tuple<ExtendedRecord,LocationRecord> source, OSGridRecord osGridRecord) {

        ExtendedRecord extendedRecord = source.v1();
        LocationRecord locationRecord = source.v2();
        Double coordinateUncertaintyToUse = osGridRecord.getCoordinateUncertaintyInMeters() != null ? osGridRecord.getCoordinateUncertaintyInMeters() : locationRecord.getCoordinateUncertaintyInMeters();

        if(locationRecord.getHasCoordinate() &&
                Strings.isNullOrEmpty(osGridRecord.getGridReference()) &&
                coordinateUncertaintyToUse != null &&
                coordinateUncertaintyToUse > 0
        ) {
            List<String> gbList = Arrays.asList("Wales", "Scotland", "England", "Isle of Man"); //OSGB-grid countries hard-coded
            List<String> niList = Arrays.asList("Northern Ireland"); //Irish grid

            String gridCalc;
            String gridToUse = "OSGB";

            if (gbList.contains(locationRecord.getStateProvince())) {
                gridToUse = "OSGB";
            } else if (niList.contains(locationRecord.getStateProvince()) || isIrishLatLon(locationRecord.getDecimalLongitude(), locationRecord.getDecimalLatitude())) {
                gridToUse = "Irish";
            }

            //double coordinateUncertaintyToUse = gridReferenceRecord.getCoordinateUncertaintyInMeters() != null ? gridReferenceRecord.getCoordinateUncertaintyInMeters() : locationRecord.getCoordinateUncertaintyInMeters();
            if (osGridRecord.getGridSizeInMeters() != null) {
                gridCalc = scalaOptionToString(GridUtil.latLonToOsGrid(locationRecord.getDecimalLatitude(), locationRecord.getDecimalLongitude(), coordinateUncertaintyToUse, "WGS84", gridToUse, osGridRecord.getGridSizeInMeters()));
            } else {
                //todo we could cobmine these two calls as does seem to allow optional argument to be ommited so just supplying default
                gridCalc = scalaOptionToString(GridUtil.latLonToOsGrid(locationRecord.getDecimalLatitude(), locationRecord.getDecimalLongitude(), coordinateUncertaintyToUse, "WGS84", gridToUse, -1));
            }

            if (!Strings.isNullOrEmpty(gridCalc)) {
                osGridRecord.setGridReference(gridCalc);

                //todo - what about northing and easting? this is how it's alway been though
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
        return !Strings.isNullOrEmpty(extractNullAwareExtensionTerm(extendedRecord, OSGridTerm.gridReference));
    }

    private static boolean suppliedWithLatLon(ExtendedRecord extendedRecord) {
        return !Strings.isNullOrEmpty(extractNullAwareValue(extendedRecord, DwcTerm.decimalLatitude)) &&
                !Strings.isNullOrEmpty(extractNullAwareValue(extendedRecord, DwcTerm.decimalLongitude));
    }

    public static void processGridWKT(Tuple<ExtendedRecord,LocationRecord> source, OSGridRecord gridReferenceRecord) {

        ExtendedRecord extendedRecord = source.v1();

        if(suppliedWithLatLon(extendedRecord)) {
            return;
        }

        //surely this will alway be empty in pipelines land
        if(Strings.isNullOrEmpty(gridReferenceRecord.getGridReferenceWKT()) &&
                suppliedWithGridReference(extendedRecord)) {

            boolean computed = false;

            String gridReference = extractNullAwareExtensionTerm(extendedRecord, OSGridTerm.gridReference);

            //should this not be empty checked as well?
            if (gridReferenceRecord.getGridReference() != null) {
                gridReferenceRecord.setGridReferenceWKT(GridUtil.getGridWKT(gridReferenceRecord.getGridReference()));
                computed = true;
            }
            //should this not be empty checked as well? and if so surely this is always true as it was previously empty checked
            //and if so what's the point of computed?
            else if (gridReference != null) {
                gridReferenceRecord.setGridReferenceWKT(GridUtil.getGridWKT(gridReference));
                computed = true;
            }

            //todo - why is this being added to the sds term informationWithheld?
//            if (computed) {
//                if (processed.occurrence.informationWithheld == null)
//                    processed.occurrence.informationWithheld = ""
//                else
//                    processed.occurrence.informationWithheld = processed.occurrence.informationWithheld + " "
//
//                processed.occurrence.informationWithheld = processed.occurrence.informationWithheld + GridUtil.getGridAsTextWithAnnotation(raw.location.gridReference)
//                // note, we don't overwrite raw.occurrence.informationWithheld, as we might prefer that untouched
//            }
        }
    }

    private static Double roundToDecimalPlaces(double input, int decimalPlaces)
    {
        double shift = Math.pow(100, decimalPlaces);
        return Math.round(input * shift) / shift;
    }
}
