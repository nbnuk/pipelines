package uk.org.nbn.pipelines;

import static au.org.ala.pipelines.transforms.IndexValues.PIPELINES_GEODETIC_DATUM;
import static org.gbif.pipelines.core.utils.ModelUtils.extractNullAwareValue;
import static uk.org.nbn.util.NBNModelUtils.getListFromString;

import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Assert;
import org.junit.Test;
import uk.org.nbn.pipelines.transforms.OSGridExtensionTransform;
import uk.org.nbn.pipelines.vocabulary.NBNOccurrenceIssue;
import uk.org.nbn.term.OSGridTerm;

@Slf4j
public class OSGridExtensionTransformTests extends OSGridTestBase {

  @Test
  public void latLonSetFromGridReferenceWhenNotSupplied() {
    ExtendedRecord er = createTestRecord();
    Map<String, String> coreTerms = er.getCoreTerms();

    coreTerms.put(OSGridTerm.gridReference.qualifiedName(), "NM39");

    OSGridExtensionTransform transform = new OSGridExtensionTransform();
    ExtendedRecord result = transform.process(er);

    Assert.assertEquals(
        "56.970009", result.getCoreTerms().get(DwcTerm.decimalLatitude.qualifiedName()));
    Assert.assertEquals(
        "-6.361995", result.getCoreTerms().get(DwcTerm.decimalLongitude.qualifiedName()));

    String osGridIssuesTerm = extractNullAwareValue(result, OSGridTerm.issues);
    Assert.assertNotNull(osGridIssuesTerm);

    List<String> osGridIssues = getListFromString(osGridIssuesTerm);
    Assert.assertTrue(
            "Check DECIMAL_LAT_LONG_CALCULATED_FROM_GRID_REF issue added",
        osGridIssues.contains(NBNOccurrenceIssue.DECIMAL_LAT_LONG_CALCULATED_FROM_GRID_REF.name()));

    Assert.assertFalse(
            "Check GRID_REF_INVALID issue not added",
        osGridIssues.contains(NBNOccurrenceIssue.GRID_REF_INVALID.name()));
  }

  @Test
  public void geodeticDatumSetWhenLatLonSetFromGridReference() {
    ExtendedRecord er = createTestRecord();
    Map<String, String> coreTerms = er.getCoreTerms();

    coreTerms.put(OSGridTerm.gridReference.qualifiedName(), "NM39");

    OSGridExtensionTransform transform = new OSGridExtensionTransform();
    ExtendedRecord result = transform.process(er);

    Assert.assertEquals(
        PIPELINES_GEODETIC_DATUM, result.getCoreTerms().get(DwcTerm.geodeticDatum.qualifiedName()));
  }

  @Test
  public void suppliedCoordinateUncertaintyRetainedWhenLatLonIsNotCentroidOfGrid() {
    ExtendedRecord er = createTestRecord();
    Map<String, String> coreTerms = er.getCoreTerms();

    coreTerms.put(OSGridTerm.gridReference.qualifiedName(), "NM39");

    final String falseCooridinateUncertainty = "9999";
    coreTerms.put(DwcTerm.decimalLatitude.qualifiedName(), "56.970009");
    coreTerms.put(DwcTerm.decimalLongitude.qualifiedName(), "-6.7");
    coreTerms.put(
        DwcTerm.coordinateUncertaintyInMeters.qualifiedName(), falseCooridinateUncertainty);

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
    Map<String, String> coreTerms = er.getCoreTerms();

    coreTerms.put(OSGridTerm.gridReference.qualifiedName(), "NM39");

    OSGridExtensionTransform transform = new OSGridExtensionTransform();
    ExtendedRecord result = transform.process(er);

    Assert.assertNotSame(er, result);
  }


  @Test
  public void issueSetWhenGridReferenceInvalid() {
    ExtendedRecord er = createTestRecord();
    Map<String, String> coreTerms = er.getCoreTerms();

    coreTerms.put(OSGridTerm.gridReference.qualifiedName(), "XXXX");

    OSGridExtensionTransform transform = new OSGridExtensionTransform();
    ExtendedRecord result = transform.process(er);

    String osGridIssuesTerm = extractNullAwareValue(result, OSGridTerm.issues);
    Assert.assertNotNull(osGridIssuesTerm);

    List<String> osGridIssues = getListFromString(osGridIssuesTerm);
    Assert.assertTrue(
            "Check GRID_REF_INVALID issue set",
            osGridIssues.contains(NBNOccurrenceIssue.GRID_REF_INVALID.name()));
  }
}
