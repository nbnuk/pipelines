package uk.org.nbn.pipelines;

import static au.org.ala.pipelines.transforms.IndexValues.PIPELINES_GEODETIC_DATUM;
import static uk.org.nbn.util.NBNModelUtils.extractNullAwareExtensionTermValue;
import static uk.org.nbn.util.NBNModelUtils.getListFromString;

import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Assert;
import org.junit.Test;
import uk.org.nbn.pipelines.transforms.OSGridExtensionTransform;
import uk.org.nbn.term.OSGridTerm;
import uk.org.nbn.vocabulary.NBNOccurrenceIssue;

@Slf4j
public class OSGridExtensionTransformTests extends OSGridTestBase {

  @Test
  public void latLonSetFromGridReferenceWhenNotSupplied() {
    ExtendedRecord er = createTestRecord();
    Map<String, String> osGridMap = getOSGridTerms(er);

    osGridMap.put(OSGridTerm.gridReference.qualifiedName(), "NM39");

    OSGridExtensionTransform transform = new OSGridExtensionTransform();
    ExtendedRecord result = transform.process(er);

    Assert.assertEquals(
        "56.970009", result.getCoreTerms().get(DwcTerm.decimalLatitude.qualifiedName()));
    Assert.assertEquals(
        "-6.361995", result.getCoreTerms().get(DwcTerm.decimalLongitude.qualifiedName()));

    String osGridIssuesTerm = extractNullAwareExtensionTermValue(result, OSGridTerm.issues);
    Assert.assertNotNull(osGridIssuesTerm);

    List<String> osGridIssues = getListFromString(osGridIssuesTerm);
    Assert.assertTrue(
        osGridIssues.contains(NBNOccurrenceIssue.DECIMAL_LAT_LONG_CALCULATED_FROM_GRID_REF.name()));
  }

  @Test
  public void geodeticDatumSetWhenLatLonSetFromGridReference() {
    ExtendedRecord er = createTestRecord();
    Map<String, String> osGridMap = getOSGridTerms(er);

    osGridMap.put(OSGridTerm.gridReference.qualifiedName(), "NM39");

    OSGridExtensionTransform transform = new OSGridExtensionTransform();
    ExtendedRecord result = transform.process(er);

    Assert.assertEquals(
        PIPELINES_GEODETIC_DATUM, result.getCoreTerms().get(DwcTerm.geodeticDatum.qualifiedName()));
  }

  @Test
  public void coordinateUncertaintySetFromGridReferenceWhenNotPresent() {
    ExtendedRecord er = createTestRecord();
    Map<String, String> osGridMap = getOSGridTerms(er);

    osGridMap.put(OSGridTerm.gridReference.qualifiedName(), "NM39");

    OSGridExtensionTransform transform = new OSGridExtensionTransform();
    ExtendedRecord result = transform.process(er);

    Assert.assertEquals(
        "7071.1", result.getCoreTerms().get(DwcTerm.coordinateUncertaintyInMeters.qualifiedName()));
  }

  @Test
  public void coordinateUncertaintySetFromGridReferenceWhenLatLonIsCentroid() {
    ExtendedRecord er = createTestRecord();
    Map<String, String> osGridMap = getOSGridTerms(er);
    Map<String, String> coreMap = er.getCoreTerms();

    osGridMap.put(OSGridTerm.gridReference.qualifiedName(), "NM39");

    final String falseCooridinateUncertainty = "9999";
    coreMap.put(DwcTerm.decimalLatitude.qualifiedName(), "56.970009");
    coreMap.put(DwcTerm.decimalLongitude.qualifiedName(), "-6.361995");
    coreMap.put(DwcTerm.coordinateUncertaintyInMeters.qualifiedName(), falseCooridinateUncertainty);

    OSGridExtensionTransform transform = new OSGridExtensionTransform();
    ExtendedRecord result = transform.process(er);

    Assert.assertEquals(
        "7071.1", result.getCoreTerms().get(DwcTerm.coordinateUncertaintyInMeters.qualifiedName()));
  }

  @Test
  public void suppliedCoordinateUncertaintyRetainedWhenLatLonIsNotCentroidOfGrid() {
    ExtendedRecord er = createTestRecord();
    Map<String, String> osGridMap = getOSGridTerms(er);
    Map<String, String> coreMap = er.getCoreTerms();

    osGridMap.put(OSGridTerm.gridReference.qualifiedName(), "NM39");

    final String falseCooridinateUncertainty = "9999";
    coreMap.put(DwcTerm.decimalLatitude.qualifiedName(), "56.970009");
    coreMap.put(DwcTerm.decimalLongitude.qualifiedName(), "-6.7");
    coreMap.put(DwcTerm.coordinateUncertaintyInMeters.qualifiedName(), falseCooridinateUncertainty);

    OSGridExtensionTransform transform = new OSGridExtensionTransform();
    ExtendedRecord result = transform.process(er);

    Assert.assertEquals(
        falseCooridinateUncertainty,
        result.getCoreTerms().get(DwcTerm.coordinateUncertaintyInMeters.qualifiedName()));
  }

  @Test
  public void sourceRecordReturnedWhenNoGridReferenceSupplied() {
    ExtendedRecord er = createTestRecord();

    OSGridExtensionTransform transform = new OSGridExtensionTransform();
    ExtendedRecord result = transform.process(er);

    Assert.assertSame(er, result);
  }

  @Test
  public void newRecordReturnedWhenGridReferenceSupplied() {
    ExtendedRecord er = createTestRecord();
    Map<String, String> osGridMap = getOSGridTerms(er);

    osGridMap.put(OSGridTerm.gridReference.qualifiedName(), "NM39");

    OSGridExtensionTransform transform = new OSGridExtensionTransform();
    ExtendedRecord result = transform.process(er);

    Assert.assertNotSame(er, result);
  }
}
