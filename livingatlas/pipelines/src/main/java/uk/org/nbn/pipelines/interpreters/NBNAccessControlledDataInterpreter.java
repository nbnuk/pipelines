package uk.org.nbn.pipelines.interpreters;

import au.org.ala.sds.generalise.FieldAccessor;
import java.util.*;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.vocabulary.InterpretationRemark;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.pipelines.core.utils.ModelUtils;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.NBNAccessControlledRecord;
import uk.org.nbn.term.OSGridTerm;
import uk.org.nbn.util.GeneralisedLocation;
import uk.org.nbn.util.GridUtil;
import uk.org.nbn.util.ScalaToJavaUtil;

/** Sensitive data interpretation methods. */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class NBNAccessControlledDataInterpreter {
  protected static final TermFactory TERM_FACTORY = TermFactory.instance();

  protected static final FieldAccessor DATA_GENERALIZATIONS =
      new FieldAccessor(DwcTerm.dataGeneralizations);
  protected static final FieldAccessor INFORMATION_WITHHELD =
      new FieldAccessor(DwcTerm.informationWithheld);
  protected static final FieldAccessor GENERALISATION_TO_APPLY_IN_METRES =
      new FieldAccessor(TERM_FACTORY.findTerm("generalisationToApplyInMetres"));
  protected static final FieldAccessor GENERALISATION_IN_METRES =
      new FieldAccessor(TERM_FACTORY.findTerm("generalisationInMetres"));
  protected static final FieldAccessor DECIMAL_LATITUDE =
      new FieldAccessor(DwcTerm.decimalLatitude);
  protected static final FieldAccessor DECIMAL_LONGITUDE =
      new FieldAccessor(DwcTerm.decimalLongitude);
  protected static final double UNALTERED = 0.000001;

  /** Bits to skip when generically updating the temporal record */
  private static final Set<Term> SKIP_TEMPORAL_UPDATE = Collections.singleton(DwcTerm.eventDate);

  /**
   * Apply access control data changes to an AVRO location record.
   *
   * @param sr The access controlled record
   * @param locationRecord A location record
   */
  public static void applyAccessControls(
      NBNAccessControlledRecord sr, LocationRecord locationRecord) {
    Map<String, String> altered = sr.getAltered();

    if (altered == null || altered.isEmpty()) {
      return;
    }
    if (altered.containsKey(DwcTerm.decimalLatitude.simpleName())) {
      locationRecord.setDecimalLatitude(
          Double.parseDouble(altered.get(DwcTerm.decimalLatitude.simpleName())));
    }
    if (altered.containsKey(DwcTerm.decimalLongitude.simpleName())) {
      locationRecord.setDecimalLongitude(
          Double.parseDouble(altered.get(DwcTerm.decimalLongitude.simpleName())));
    }
    if (altered.containsKey(DwcTerm.coordinateUncertaintyInMeters.simpleName())) {
      locationRecord.setCoordinateUncertaintyInMeters(
          Double.parseDouble(altered.get(DwcTerm.coordinateUncertaintyInMeters.simpleName())));
    }
    if (altered.containsKey(DwcTerm.locality.simpleName())) {
      locationRecord.setLocality(altered.get(DwcTerm.locality.simpleName()));
    }
    if (altered.containsKey(DwcTerm.footprintWKT.simpleName())) {
      locationRecord.setFootprintWKT(altered.get(DwcTerm.footprintWKT.simpleName()));
    }
  }

  /**
   * @param sr The access controlled record
   * @param osGridRecord An OS grid record
   */
  public static void applyAccessControls(NBNAccessControlledRecord sr, OSGridRecord osGridRecord) {
    Map<String, String> altered = sr.getAltered();

    if (altered == null || altered.isEmpty()) {
      return;
    }
    if (altered.containsKey(OSGridTerm.gridReference.simpleName())) {
      osGridRecord.setGridReference(altered.get(OSGridTerm.gridReference.simpleName()));
    }
    if (altered.containsKey(OSGridTerm.gridSizeInMeters.simpleName())) {
      osGridRecord.setGridSizeInMeters(
          Integer.parseInt(altered.get(OSGridTerm.gridSizeInMeters.simpleName())));
    }
  }

  private static void replaceOrRemove(Map<String, String> coreTerms, String key, String value) {
    if (value == null) {
      coreTerms.remove(key);
    } else {
      coreTerms.put(key, value);
    }
  }
  /**
   * Apply access control data changes to an AVRO extended record.
   *
   * @param sr The access controlled record
   * @param extendedRecord An extended record
   */
  public static void applyAccessControls(
      NBNAccessControlledRecord sr, ExtendedRecord extendedRecord) {
    Map<String, String> altered = sr.getAltered();

    if (altered == null || altered.isEmpty()) {
      return;
    }
    // if null, delete from extendedrecord otherwise the extended record value will get written
    // to the index
    replaceOrRemove(
        extendedRecord.getCoreTerms(),
        DwcTerm.decimalLatitude.qualifiedName(),
        altered.get(DwcTerm.decimalLatitude.simpleName()));
    replaceOrRemove(
        extendedRecord.getCoreTerms(),
        DwcTerm.decimalLongitude.qualifiedName(),
        altered.get(DwcTerm.decimalLongitude.simpleName()));
    replaceOrRemove(
        extendedRecord.getCoreTerms(),
        DwcTerm.coordinateUncertaintyInMeters.qualifiedName(),
        altered.get(DwcTerm.coordinateUncertaintyInMeters.simpleName()));
    replaceOrRemove(
        extendedRecord.getCoreTerms(),
        OSGridTerm.gridReference.qualifiedName(),
        altered.get(OSGridTerm.gridReference.simpleName()));
    replaceOrRemove(
        extendedRecord.getCoreTerms(),
        OSGridTerm.gridSizeInMeters.qualifiedName(),
        altered.get(OSGridTerm.gridSizeInMeters.simpleName()));
    replaceOrRemove(
        extendedRecord.getCoreTerms(), DwcTerm.locality.qualifiedName(), altered.get("locality"));
    replaceOrRemove(
        extendedRecord.getCoreTerms(),
        DwcTerm.verbatimLatitude.qualifiedName(),
        altered.get(DwcTerm.verbatimLatitude.simpleName()));
    replaceOrRemove(
        extendedRecord.getCoreTerms(),
        DwcTerm.verbatimLongitude.qualifiedName(),
        altered.get(DwcTerm.verbatimLongitude.simpleName()));
    replaceOrRemove(
        extendedRecord.getCoreTerms(),
        DwcTerm.verbatimLocality.qualifiedName(),
        altered.get(DwcTerm.verbatimLocality.simpleName()));
    replaceOrRemove(
        extendedRecord.getCoreTerms(),
        DwcTerm.verbatimCoordinates.qualifiedName(),
        altered.get(DwcTerm.verbatimCoordinates.simpleName()));
    replaceOrRemove(
        extendedRecord.getCoreTerms(),
        DwcTerm.footprintWKT.qualifiedName(),
        altered.get(DwcTerm.footprintWKT.simpleName()));
    replaceOrRemove(
        extendedRecord.getCoreTerms(),
        DwcTerm.locationRemarks.qualifiedName(),
        altered.get(DwcTerm.locationRemarks.simpleName()));
    replaceOrRemove(
        extendedRecord.getCoreTerms(),
        DwcTerm.occurrenceRemarks.qualifiedName(),
        altered.get(DwcTerm.occurrenceRemarks.simpleName()));
  }

  private static Map<String, String> blur(
      Map<String, String> original, int publicResolutionToBeApplied) {

    Map<String, String> blurred = new HashMap<>();

    if (original.get("decimalLatitude") != null && original.get("decimalLongitude") != null) {
      GeneralisedLocation generalisedLocation =
          new GeneralisedLocation(
              original.get("decimalLatitude"),
              original.get("decimalLongitude"),
              publicResolutionToBeApplied);
      blurred.put("decimalLatitude", generalisedLocation.getGeneralisedLatitude());
      blurred.put("decimalLongitude", generalisedLocation.getGeneralisedLongitude());
    }

    if (original.get("gridReference") != null && !original.get("gridReference").isEmpty()) {
      blurred.put(
          "gridReference",
          ScalaToJavaUtil.scalaOptionToString(
              GridUtil.convertReferenceToResolution(
                  original.get("gridReference"), String.valueOf(publicResolutionToBeApplied))));
    }

    String blurredCoordinateUncertainty =
        GridUtil.gridToCoordinateUncertaintyString(publicResolutionToBeApplied);

    if ((original.get("coordinateUncertaintyInMeters") != null
            && !original.get("coordinateUncertaintyInMeters").isEmpty()
            && (java.lang.Double.parseDouble(original.get("coordinateUncertaintyInMeters"))
                < java.lang.Double.parseDouble(blurredCoordinateUncertainty)))
        || (original.get("coordinateUncertaintyInMeters") == null
            || original.get("coordinateUncertaintyInMeters").isEmpty())) {
      blurred.put("coordinateUncertaintyInMeters", blurredCoordinateUncertainty);
    }

    if (original.get("gridSizeInMeters") != null
        && !original.get("gridSizeInMeters").isEmpty()
        && java.lang.Integer.parseInt(original.get("gridSizeInMeters"))
            < publicResolutionToBeApplied) {
      blurred.put("gridSizeInMeters", String.valueOf(publicResolutionToBeApplied));
    }

    // clear the remaining access controlled values
    blurred.put("locality", "");
    blurred.put("verbatimLatitude", "");
    blurred.put("verbatimLongitude", "");
    blurred.put("verbatimLocality", "");
    blurred.put("verbatimCoordinates", "");
    blurred.put("footprintWKT", "");
    blurred.put("locationRemarks", "");
    //    blurred.put("occurrenceRemarks", "");

    return blurred;
  }

  /**
   * Interprets a utils from the taxonomic properties supplied from the various source records.
   *
   * @param dataResourceUid The sensitive species lookup
   * @param accessControlledRecord The sensitive data report
   */
  public static void accessControlledDataInterpreter(
      String dataResourceUid,
      Integer publicResolutionToApplyInMeters,
      ExtendedRecord extendedRecord,
      LocationRecord locationRecord,
      OSGridRecord osGridRecord,
      NBNAccessControlledRecord accessControlledRecord) {

    accessControlledRecord.setAccessControlled(publicResolutionToApplyInMeters > 0);

    if (publicResolutionToApplyInMeters > 0) {

      Map<String, String> original = new HashMap<>();

      original.put(
          "decimalLatitude",
          locationRecord.getDecimalLatitude() != null
              ? locationRecord.getDecimalLatitude().toString()
              : null);
      original.put(
          "decimalLongitude",
          locationRecord.getDecimalLongitude() != null
              ? locationRecord.getDecimalLongitude().toString()
              : null);
      original.put(
          "coordinateUncertaintyInMeters",
          locationRecord.getCoordinateUncertaintyInMeters() != null
              ? locationRecord.getCoordinateUncertaintyInMeters().toString()
              : null);
      original.put("footprintWKT", locationRecord.getFootprintWKT());

      original.put("gridReference", osGridRecord.getGridReference());
      original.put(
          "gridSizeInMeters",
          osGridRecord.getGridSizeInMeters() != null
              ? osGridRecord.getGridSizeInMeters().toString()
              : null);
      original.put("locality", locationRecord.getLocality());
      original.put(
          "verbatimLatitude",
          extendedRecord.getCoreTerms().get(DwcTerm.verbatimLatitude.qualifiedName()));
      original.put(
          "verbatimLongitude",
          extendedRecord.getCoreTerms().get(DwcTerm.verbatimLongitude.qualifiedName()));
      original.put(
          "verbatimLocality",
          extendedRecord.getCoreTerms().get(DwcTerm.verbatimLocality.qualifiedName()));
      original.put(
          "verbatimCoordinates",
          extendedRecord.getCoreTerms().get(DwcTerm.verbatimCoordinates.qualifiedName()));

      original.put(
          "locationRemarks",
          extendedRecord.getCoreTerms().get(DwcTerm.locationRemarks.qualifiedName()));
      //      original.put(
      //          "occurrenceRemarks",
      //          extendedRecord.getCoreTerms().get(DwcTerm.occurrenceRemarks.qualifiedName()));

      Map<String, String> blurred = blur(original, publicResolutionToApplyInMeters);

      // TODO this is not in phase1 so dont implement it yet
      //      accessControlledRecord.setDataGeneralizations(
      //              "Public resolution of "+dataResourceNbn.getPublicResolutionToBeApplied()+"m
      // applied");
      //      accessControlledRecord.setInformationWithheld(
      //
      // INFORMATION_WITHHELD.get(result).getValue().map(Object::toString).orElse(null));

      accessControlledRecord.setPublicResolutionInMetres(
          publicResolutionToApplyInMeters.toString());
      accessControlledRecord.setOriginal(toStringMap(original));
      accessControlledRecord.setAltered(toStringMap(blurred));
    }
  }

  /**
   * Add an issue to the issues list.
   *
   * @param sr The record
   * @param issue The issue
   */
  protected static void addIssue(NBNAccessControlledRecord sr, InterpretationRemark issue) {
    ModelUtils.addIssue(sr, issue.getId());
  }

  /** Convert a map into a map of string key-values. */
  protected static <K, V> Map<String, String> toStringMap(Map<K, V> original) {
    Map<String, String> strings = new HashMap<>(original.size());
    for (Map.Entry<K, V> entry : original.entrySet()) {
      strings.put(
          entry.getKey().toString(), entry.getValue() == null ? null : entry.getValue().toString());
    }
    return strings;
  }
}
