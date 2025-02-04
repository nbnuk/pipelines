package uk.org.nbn.pipelines;

import au.org.ala.pipelines.vocabulary.ALAOccurrenceIssue;
import java.util.HashMap;
import java.util.Map;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.junit.Assert;
import org.junit.Test;
import uk.org.nbn.pipelines.interpreters.NBNLocationInterpreter;
import uk.org.nbn.term.OSGridTerm;

public class NBNLocationInterpreterTests extends OSGridTestBase {

  @Test
  public void issueNotAddedWhenPrecisionValueDoesNotEndInKmOrM() {

    ExtendedRecord er = createTestRecord();
    er.getCoreTerms().put(DwcTerm.coordinatePrecision.qualifiedName(), "2x");

    LocationRecord lr = LocationRecord.newBuilder().setId(ID).build();

    NBNLocationInterpreter.interpretCoordinateUncertaintyInMetersFromPrecisionFormat(er, lr);

    Assert.assertFalse(
        lr.getIssues().getIssueList().contains(ALAOccurrenceIssue.UNCERTAINTY_IN_PRECISION.name()));
  }

  @Test
  public void issueAddedWhenCoordinateUncertaintyInPrecision() {

    // Work around to avoid need to install the org.junit.jupiter.params.ParameterizedTest package
    Map<String, Boolean> testCases = new HashMap<>();
    testCases.put("2km", true);
    testCases.put("1km", true);
    testCases.put("2m", true);
    testCases.put("1m", false);

    testCases.forEach(
        (key, value) -> {
          issueAddedWhenCoordinateUncertaintyInPrecisionUnless1m(key, value);
        });
  }

  public void issueAddedWhenCoordinateUncertaintyInPrecisionUnless1m(
      String precisionValue, boolean assertionExpected) {

    ExtendedRecord er = createTestRecord();
    er.getCoreTerms().put(DwcTerm.coordinatePrecision.qualifiedName(), precisionValue);

    LocationRecord lr = LocationRecord.newBuilder().setId(ID).build();

    NBNLocationInterpreter.interpretCoordinateUncertaintyInMetersFromPrecisionFormat(er, lr);

    Assert.assertEquals(
        assertionExpected,
        lr.getIssues().getIssueList().contains(ALAOccurrenceIssue.UNCERTAINTY_IN_PRECISION.name()));
  }

  @Test
  public void coordinateUncertaintySetFromGridReferenceWhenNotPresent() {
    ExtendedRecord er = createTestRecord();
    Map<String, String> coreTerms = er.getCoreTerms();

    coreTerms.put(OSGridTerm.gridReference.qualifiedName(), "NM39");

    LocationRecord lr = LocationRecord.newBuilder().setId(ID).build();
    lr.getIssues().getIssueList().add(OccurrenceIssue.COORDINATE_UNCERTAINTY_METERS_INVALID.name());

    NBNLocationInterpreter.interpretCoordinateUncertaintyInMeters(er, lr);

    Double expectedCoordinateUncertainty = 7071.1;
    Assert.assertEquals(expectedCoordinateUncertainty, lr.getCoordinateUncertaintyInMeters());
    Assert.assertFalse(
        "Check uncertainty issue removed",
        lr.getIssues()
            .getIssueList()
            .contains(OccurrenceIssue.COORDINATE_UNCERTAINTY_METERS_INVALID.name()));
  }

  @Test
  public void coordinateUncertaintySetFromGridReferenceWhenLatLonIsCentroid() {
    ExtendedRecord er = createTestRecord();
    Map<String, String> coreTerms = er.getCoreTerms();

    coreTerms.put(OSGridTerm.gridReference.qualifiedName(), "NM39");

    final String falseCooridinateUncertainty = "9999";
    coreTerms.put(DwcTerm.decimalLatitude.qualifiedName(), "56.970009");
    coreTerms.put(DwcTerm.decimalLongitude.qualifiedName(), "-6.361995");
    coreTerms.put(
        DwcTerm.coordinateUncertaintyInMeters.qualifiedName(), falseCooridinateUncertainty);

    LocationRecord lr = LocationRecord.newBuilder().setId(ID).build();
    NBNLocationInterpreter.interpretCoordinateUncertaintyInMeters(er, lr);

    Double expectedCoordinateUncertainty = 7071.1;
    Assert.assertEquals(expectedCoordinateUncertainty, lr.getCoordinateUncertaintyInMeters());
  }

  @Test
  public void coordinateUncertaintyNullWhenSetFromInvalidGridReference() {
    ExtendedRecord er = createTestRecord();
    Map<String, String> coreTerms = er.getCoreTerms();

    coreTerms.put(OSGridTerm.gridReference.qualifiedName(), "XXXX");

    LocationRecord lr = LocationRecord.newBuilder().setId(ID).build();
    lr.getIssues().getIssueList().add(OccurrenceIssue.COORDINATE_UNCERTAINTY_METERS_INVALID.name());

    NBNLocationInterpreter.interpretCoordinateUncertaintyInMeters(er, lr);

    Assert.assertNull(lr.getCoordinateUncertaintyInMeters());
    Assert.assertTrue(
        "Check uncertainty issue removed",
        lr.getIssues()
            .getIssueList()
            .contains(OccurrenceIssue.COORDINATE_UNCERTAINTY_METERS_INVALID.name()));
  }
}
