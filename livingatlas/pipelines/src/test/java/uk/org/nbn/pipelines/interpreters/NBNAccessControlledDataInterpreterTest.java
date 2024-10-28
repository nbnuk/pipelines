package uk.org.nbn.pipelines.interpreters;

import static org.junit.Assert.*;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.NBNAccessControlledRecord;
import org.gbif.pipelines.io.avro.OSGridRecord;
import org.junit.Test;
import uk.org.nbn.term.OSGridTerm;

public class NBNAccessControlledDataInterpreterTest {

  private static final String ID = "ac:ll:notsensitive:1";

  @Test
  public void testZeroPublicResolutionToApplyInMeters() {
    // State
    NBNAccessControlledRecord accessControlledRecord =
        NBNAccessControlledRecord.newBuilder().setId(ID).build();
    ExtendedRecord extendedRecord = ExtendedRecord.newBuilder().setId(ID).build();
    LocationRecord locationRecord = LocationRecord.newBuilder().setId(ID).build();
    OSGridRecord osGridRecord = OSGridRecord.newBuilder().setId(ID).build();

    locationRecord.setDecimalLatitude(53.181506);
    locationRecord.setDecimalLongitude(-2.375537);
    locationRecord.setCoordinateUncertaintyInMeters(0.7);
    extendedRecord.getCoreTerms().put(DwcTerm.locationRemarks.qualifiedName(), "Place description");
    extendedRecord
        .getCoreTerms()
        .put(DwcTerm.occurrenceRemarks.qualifiedName(), "Occurrence description");

    Integer publicResolutionToApplyInMeters = 0;

    // When
    NBNAccessControlledDataInterpreter.accessControlledDataInterpreter(
        "8cd90eae-92e2-4515-bb7d-61ba859fb84f",
        publicResolutionToApplyInMeters,
        extendedRecord,
        locationRecord,
        osGridRecord,
        accessControlledRecord);

    // Then
    assertFalse(accessControlledRecord.getAccessControlled());
  }

  @Test
  public void testNonZeroPublicResolutionToApplyInMeters() {
    // State
    NBNAccessControlledRecord accessControlledRecord =
        NBNAccessControlledRecord.newBuilder().setId(ID).build();
    ExtendedRecord extendedRecord = ExtendedRecord.newBuilder().setId(ID).build();
    LocationRecord locationRecord = LocationRecord.newBuilder().setId(ID).build();
    OSGridRecord osGridRecord = OSGridRecord.newBuilder().setId(ID).build();

    locationRecord.setDecimalLatitude(53.181506);
    locationRecord.setDecimalLongitude(-2.375537);
    locationRecord.setCoordinateUncertaintyInMeters(0.7);
    locationRecord.setLocality("Place description");
    locationRecord.setFootprintWKT("Footprint description");

    osGridRecord.setGridSizeInMeters(1);
    osGridRecord.setGridReference("TQ35886886");

    extendedRecord.getCoreTerms().put(DwcTerm.locationRemarks.simpleName(), "Place description");
    extendedRecord.getCoreTerms().put(DwcTerm.verbatimLatitude.qualifiedName(), "53.2");
    extendedRecord.getCoreTerms().put(DwcTerm.verbatimLongitude.qualifiedName(), "-2.4");
    extendedRecord.getCoreTerms().put(DwcTerm.verbatimLocality.qualifiedName(), "verbatimLocality");
    extendedRecord.getCoreTerms().put(DwcTerm.verbatimCoordinates.qualifiedName(), "53.2, -2.4");
    extendedRecord.getCoreTerms().put(DwcTerm.locationRemarks.qualifiedName(), "locationRemarks");

    Integer publicResolutionToApplyInMeters = 10000;

    // When
    NBNAccessControlledDataInterpreter.accessControlledDataInterpreter(
        "8cd90eae-92e2-4515-bb7d-61ba859fb84f",
        publicResolutionToApplyInMeters,
        extendedRecord,
        locationRecord,
        osGridRecord,
        accessControlledRecord);

    // Then
    assertTrue(accessControlledRecord.getAccessControlled());
    assertEquals("10000", accessControlledRecord.getPublicResolutionInMetres());
    assertEquals(
        "-2.4", accessControlledRecord.getAltered().get(DwcTerm.decimalLongitude.simpleName()));
    assertEquals(
        "53.2", accessControlledRecord.getAltered().get(DwcTerm.decimalLatitude.simpleName()));
    assertEquals(
        "7071.1",
        accessControlledRecord
            .getAltered()
            .get(DwcTerm.coordinateUncertaintyInMeters.simpleName()));
    assertEquals(
        "TQ36", accessControlledRecord.getAltered().get(OSGridTerm.gridReference.simpleName()));
    assertEquals(
        "10000", accessControlledRecord.getAltered().get(OSGridTerm.gridSizeInMeters.simpleName()));
    assertEquals("", accessControlledRecord.getAltered().get(DwcTerm.locality.simpleName()));
    assertEquals("", accessControlledRecord.getAltered().get(DwcTerm.footprintWKT.simpleName()));
    assertEquals(
        "", accessControlledRecord.getAltered().get(DwcTerm.verbatimLatitude.simpleName()));
    assertEquals(
        "", accessControlledRecord.getAltered().get(DwcTerm.verbatimLongitude.simpleName()));
    assertEquals(
        "", accessControlledRecord.getAltered().get(DwcTerm.verbatimLocality.simpleName()));
    assertEquals(
        "", accessControlledRecord.getAltered().get(DwcTerm.verbatimCoordinates.simpleName()));
    assertEquals("", accessControlledRecord.getAltered().get(DwcTerm.locationRemarks.simpleName()));
  }
}
