package uk.org.nbn.pipelines.interpreters;

import au.org.ala.pipelines.parser.CoordinatesParser;
import com.google.common.base.Strings;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.elasticsearch.common.collect.Tuple;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.geospatial.MeterRangeParser;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.core.parsers.common.ParsedField;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GridReferenceRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.rest.client.geocode.GeocodeResponse;
import scala.Option;
import uk.org.nbn.term.OSGridTerm;
import uk.org.nbn.util.GISPoint;
import uk.org.nbn.util.GridUtil;
import uk.org.nbn.util.ScalaToJavaUtil;
import uk.org.nbn.vocabulary.NBNOccurrenceIssue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.gbif.api.vocabulary.OccurrenceIssue.COORDINATE_UNCERTAINTY_METERS_INVALID;

import static org.gbif.pipelines.core.utils.ModelUtils.*;

public class GridReferenceInterpreter {

    public static String dwcaGridReferenceExtensionType = "http://nbn.org.uk/dwc/terms/gridreference";
    public static String dwcaGridReferenceTerm = "http://unknown.org/gridReference";
//    public static void interpretGridReference(ExtendedRecord extendedRecord, GridReferenceRecord gridReferenceRecord) {
//
//            if(hasExtension(extendedRecord, dwcaGridReferenceExtensionType)){
//
//                List<Map<String,String>> gridRefernceTerms = extendedRecord.getExtensions().get(dwcaGridReferenceExtensionType);
//                Map<String, String> mergedMap = gridRefernceTerms.stream()
//                        .flatMap(map -> map.entrySet().stream())
//                        .collect(Collectors.toMap(
//                                Map.Entry::getKey,
//                                Map.Entry::getValue
//                        ));
//
//                String gridReference = mergedMap.getOrDefault(dwcaGridReferenceTerm, null);
//
//                if(gridReference != null)
//                {
//                    //store the grid reference
//                    gridReferenceRecord.setGridReference(gridReference);
//
//                    //gridReferenceRecord.setGridSizeInMeters(ScalaToJavaUtil.scalaOptionToInt(GridUtil.getGridSizeInMeters(gridReference)));
//
//                    //augment the extended record ready for the location transform
//                    if(requiresGridReferenceParsing(extendedRecord))
//                    {
//                        Option<GISPoint> result = GridUtil.processGridReference(gridReference);
//                        if (!result.isEmpty()) {
//
//                            //todo - add assertion
//                            //addIssue();
//                            //assertions += QualityAssertion(DECIMAL_LAT_LONG_CALCULATED_FROM_GRID_REF)
//
//                            GISPoint gisPoint = result.get();
//                            Map<String, String> coreTerms = extendedRecord.getCoreTerms();
//
//                            coreTerms.put(DwcTerm.decimalLatitude.qualifiedName(), gisPoint.latitude());
//                            coreTerms.put(DwcTerm.decimalLongitude.qualifiedName(), gisPoint.longitude());
//                            coreTerms.put(DwcTerm.geodeticDatum.qualifiedName(), gisPoint.datum());
//                            coreTerms.put(DwcTerm.coordinateUncertaintyInMeters.qualifiedName(), gisPoint.coordinateUncertaintyInMeters());
//                            //todo - do we need to store easting and northing?
//                        }
//                    }
//                } else {
//                    ParsedField<LatLng> parsedLatLon = CoordinatesParser.parseCoords(extendedRecord);
//                    if(parsedLatLon.isSuccessful())
//                    {
//
//
//                    }
//                }
//            }
//            //addIssue(lr, parsedLatLon.getIssues());
//    }


    public static void addGridSize(Tuple<ExtendedRecord,LocationRecord> source, GridReferenceRecord gridReferenceRecord) {

        ExtendedRecord extendedRecord = source.v1();

        String gridReference = extractNullAwareValue(extendedRecord, OSGridTerm.gridReference);
        String gridSizeInMeters = extractNullAwareValue(extendedRecord, OSGridTerm.gridSizeInMeters);

        // this doesn't feel necessarry as we're no longer mutating in place
        if(gridReferenceRecord.getGridSizeInMeters() == null)
        {
            if(!Strings.isNullOrEmpty(gridReferenceRecord.getGridReference())) {
                //don't think this should ever happen as we're no longer mutating in place
                Integer computedGridSizeInMeters = GridUtil.getGridSizeInMeters(gridReferenceRecord.getGridReference()).getOrElse(null);
                gridReferenceRecord.setGridSizeInMeters(computedGridSizeInMeters);

            } else if (!Strings.isNullOrEmpty(gridReference)) {
                Integer computedGridSizeInMeters = GridUtil.getGridSizeInMeters(gridReference).getOrElse(null);
                gridReferenceRecord.setGridSizeInMeters(computedGridSizeInMeters);

            } else if (!Strings.isNullOrEmpty(gridSizeInMeters)) {
                Integer computedGridSizeInMeters =  Ints.tryParse(gridSizeInMeters);
                gridReferenceRecord.setGridSizeInMeters(computedGridSizeInMeters);

            }

            // As GridSizeInMeters is Integer it cannot be empty so need to set null here anymore
        }
    }

    public static void possiblyRecalculateCoordinateUncertainty(Tuple<ExtendedRecord,LocationRecord> source, GridReferenceRecord gridReferenceRecord) {
        //if grid and no lat/long
        //or if grid and lat/long, and lat/long is centroid
        //or if grid and lat/long and no coordinate uncertainty provided
        //then amend coordinate uncertainty to radius of circle through corners of grid

        ExtendedRecord extendedRecord = source.v1();
        LocationRecord locationRecord = source.v2();

        String gridReference = extractNullAwareValue(extendedRecord, OSGridTerm.gridReference);
        String gridSizeInMeters = extractNullAwareValue(extendedRecord, OSGridTerm.gridSizeInMeters);


        if(Strings.isNullOrEmpty(gridReference)) {
            return;
        }

        String decimalLatitude = extractNullAwareValue(extendedRecord, DwcTerm.decimalLatitude);
        String decimalLongitude = extractNullAwareValue(extendedRecord, DwcTerm.decimalLongitude);

        boolean hasLatLon = !Strings.isNullOrEmpty(decimalLatitude) && !Strings.isNullOrEmpty(decimalLongitude);
        boolean latLonIsCentroidOfGridReference = hasLatLon && GridUtil.isCentroid(Double.valueOf(decimalLatitude), Double.valueOf(decimalLongitude), gridReference);

        // we could check the Extended Record for this however location processor does try to parse this and coordinatePrecision
        boolean latLonButNoUncertaintySupplied = hasLatLon && locationRecord.getCoordinateUncertaintyInMeters() == null;

        boolean recalcCoordUncertainty = !hasLatLon || latLonIsCentroidOfGridReference || latLonButNoUncertaintySupplied;

        if(hasLatLon && !latLonIsCentroidOfGridReference)
        {
            addIssue(gridReferenceRecord, NBNOccurrenceIssue.COORDINATES_NOT_CENTRE_OF_GRID.name());
        }

        if(recalcCoordUncertainty) {
//            val cornerDistFromCentre =
//            if (processed.location.gridSizeInMeters != null && processed.location.gridSizeInMeters.length > 0) {
//                processed.location.gridSizeInMeters.toDouble / math.sqrt(2.0)
//            } else if (raw.location.gridSizeInMeters != null && raw.location.gridSizeInMeters.length > 0) {
//                raw.location.gridSizeInMeters.toDouble / math.sqrt(2.0)
//            } else {
//                -1 //give up
//            }
//
//            if (cornerDistFromCentre >= 0)
//                processed.location.coordinateUncertaintyInMeters = "%.1f".format(cornerDistFromCentre)

            double cornerDistanceFromCentre = -1;

            if(gridReferenceRecord.getGridSizeInMeters() != null) {
                cornerDistanceFromCentre = gridReferenceRecord.getGridSizeInMeters() / Math.sqrt(2.0);
            } else if (!Strings.isNullOrEmpty(gridSizeInMeters)) {
                //todo this should never happen as the above should always be true if not then we have a problem
                cornerDistanceFromCentre = Doubles.tryParse(gridSizeInMeters);
            }

            if(cornerDistanceFromCentre >= 0) {
                //See LocationInterpreter for this handling
                ParseResult<Double> parseResult = MeterRangeParser.parseMeters(Double.toString(cornerDistanceFromCentre));
                Double result = parseResult.isSuccessful() ? Math.abs(cornerDistanceFromCentre) : null;

                if (result != null) {
                    gridReferenceRecord.setCoordinateUncertaintyInMeters(result);
                } else {
                    addIssue(gridReferenceRecord, COORDINATE_UNCERTAINTY_METERS_INVALID);
                }
            }
        }
    }

    public static void setGridRefFromCoordinates(Tuple<ExtendedRecord,LocationRecord> source, GridReferenceRecord gridReferenceRecord) {

//        if (processed.location.decimalLatitude != null
//                && processed.location.decimalLongitude != null
//                && processed.location.gridReference == null
//                && processed.location.coordinateUncertaintyInMeters != null
//                && processed.location.coordinateUncertaintyInMeters.toDouble > 0) {
//            val gbList = List("Wales", "Scotland", "England", "Isle of Man") //OSGB-grid countries hard-coded
//            val niList = List("Northern Ireland") //Irish grid
//            var gridCalc = None: Option[String]
//            var gridToUse = "OSGB" //TODO: could add Channel Islands when applicable. For now, just try OSGB grid for everything non-Irish
//            if (gbList.contains(processed.location.stateProvince)) {
//                gridToUse = "OSGB"
//            } else if (niList.contains(processed.location.stateProvince)) {
//                gridToUse = "Irish"
//            } else if ((processed.location.decimalLongitude.toDouble < -5.0) &&
//                    (processed.location.decimalLatitude.toDouble < 57.0 && processed.location.decimalLatitude.toDouble > 48.0)) {
//                gridToUse = "Irish"
//            }
//            if (processed.location.gridSizeInMeters != null && processed.location.gridSizeInMeters.length > 0) {
//                gridCalc = GridUtil.latLonToOsGrid(processed.location.decimalLatitude.toDouble, processed.location.decimalLongitude.toDouble, processed.location.coordinateUncertaintyInMeters.toDouble, "WGS84", gridToUse, processed.location.gridSizeInMeters.toInt)
//            } else {
//                gridCalc = GridUtil.latLonToOsGrid(processed.location.decimalLatitude.toDouble, processed.location.decimalLongitude.toDouble, processed.location.coordinateUncertaintyInMeters.toDouble, "WGS84", gridToUse)
//            }
//            if (gridCalc.isDefined) {
//                processed.location.gridReference = gridCalc.get
//                if (raw.location.gridReference == null || raw.location.gridReference.isEmpty) {
//                    assertions += QualityAssertion(GRID_REF_CALCULATED_FROM_LAT_LONG)
//                }
//            }
//        }

        ExtendedRecord extendedRecord = source.v1();
        LocationRecord locationRecord = source.v2();

        if(locationRecord.getHasCoordinate() &&
                Strings.isNullOrEmpty(gridReferenceRecord.getGridReference()) &&
                gridReferenceRecord.getCoordinateUncertaintyInMeters() != null
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

            double coordinateUncertaintyToUse = gridReferenceRecord.getCoordinateUncertaintyInMeters() != null ? gridReferenceRecord.getCoordinateUncertaintyInMeters() : locationRecord.getCoordinateUncertaintyInMeters();
            if (gridReferenceRecord.getGridSizeInMeters() != null) {
                gridCalc = GridUtil.latLonToOsGrid(locationRecord.getDecimalLatitude(), locationRecord.getDecimalLongitude(), coordinateUncertaintyToUse, "WGS84", gridToUse, gridReferenceRecord.getGridSizeInMeters()).getOrElse(null);
            } else {
                //todo we could cobmine these two calls as does seem to allow optional argument to be ommited so just supplying default
                gridCalc = GridUtil.latLonToOsGrid(locationRecord.getDecimalLatitude(), locationRecord.getDecimalLongitude(), coordinateUncertaintyToUse, "WGS84", gridToUse, -1).getOrElse(null);
            }

            //todo - what about northing and easting?
            boolean suppliedWithGridReference = hasValueNullAware(extendedRecord, OSGridTerm.gridReference);

            if (!Strings.isNullOrEmpty(gridCalc)) {
                gridReferenceRecord.setGridReference(gridCalc);

                if (!suppliedWithGridReference) {
                    addIssue(gridReferenceRecord, NBNOccurrenceIssue.GRID_REF_CALCULATED_FROM_LAT_LONG.name());
                }
            }
        }
    }

    private static boolean isIrishLatLon(double longitude, double latitude) {
        return longitude < -5.0 && latitude < 57.0 && latitude > 48.0;
    }

    public static void processGridWKT(ExtendedRecord extendedRecord, GridReferenceRecord gridReferenceRecord) {


    }

    //reflects other options used in biocache-store LocationProcessor::processLatLong
    public static boolean requiresGridReferenceParsing(ExtendedRecord extendedRecord){
        ParsedField<LatLng> parsedLatLon = CoordinatesParser.parseCoords(extendedRecord);
        return  !parsedLatLon.isSuccessful();
    }
}
