package uk.org.nbn.pipelines;

import static org.hamcrest.CoreMatchers.*;
import static uk.org.nbn.util.NBNModelUtils.getStringFromList;

import com.google.common.base.Strings;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.elasticsearch.common.collect.Tuple;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.OSGridRecord;
import org.junit.Assert;
import org.junit.Test;
import uk.org.nbn.pipelines.interpreters.OSGridInterpreter;
import uk.org.nbn.term.OSGridTerm;
import uk.org.nbn.pipelines.vocabulary.NBNOccurrenceIssue;

public class OSGridInterpreterTests extends OSGridTestBase {

  private Tuple<ExtendedRecord, LocationRecord> createInput(ExtendedRecord er) {
    return createInput(er, LocationRecord.newBuilder().setId(ID).build());
  }

  private Tuple<ExtendedRecord, LocationRecord> createInput(ExtendedRecord er, LocationRecord lr) {
    return new Tuple<>(er, lr);
  }

  @Test
  public void applyIssue_issuesTermAddedToOSGridRecord() {

    final List<String> expectedIssues =
        Arrays.asList(
            NBNOccurrenceIssue.GRID_REF_CALCULATED_FROM_LAT_LONG.name(),
            NBNOccurrenceIssue.COORDINATES_NOT_CENTRE_OF_GRID.name());

    ExtendedRecord er = createTestRecord();
    Map<String, String> coreTerms = er.getCoreTerms();

    coreTerms.put(OSGridTerm.issues.qualifiedName(), getStringFromList(expectedIssues));

    Tuple<ExtendedRecord, LocationRecord> input = createInput(er);

    OSGridRecord osgr = OSGridRecord.newBuilder().setId(ID).build();

    OSGridInterpreter.applyIssues(input, osgr);

    Assert.assertEquals(expectedIssues, osgr.getIssues().getIssueList());
  }

  @Test
  public void applyIssue_invalidIssuesNotAddedToOSGridRecord() {

    String invalidIssue = "INVALIDISSUE";
    String validIssue = NBNOccurrenceIssue.GRID_REF_CALCULATED_FROM_LAT_LONG.name();
    final List<String> expectedIssues = Arrays.asList(validIssue, invalidIssue);

    ExtendedRecord er = createTestRecord();
    Map<String, String> coreTerms = er.getCoreTerms();

    coreTerms.put(OSGridTerm.issues.qualifiedName(), getStringFromList(expectedIssues));

    Tuple<ExtendedRecord, LocationRecord> input = createInput(er);

    OSGridRecord osgr = OSGridRecord.newBuilder().setId(ID).build();

    OSGridInterpreter.applyIssues(input, osgr);

    Assert.assertTrue(osgr.getIssues().getIssueList().contains(validIssue));
    Assert.assertFalse(osgr.getIssues().getIssueList().contains(invalidIssue));
  }

  @Test
  public void addGrid_notChangedWhenProssedValueSet() {

    final Integer expectedGridSize = new Integer(10000);

    final String unexpectedGridReference = "NH123123";
    final Integer unexpectedGridSize = 100;

    ExtendedRecord er = createTestRecord();
    Map<String, String> coreTerms = er.getCoreTerms();

    coreTerms.put(OSGridTerm.gridSizeInMeters.qualifiedName(), String.valueOf(unexpectedGridSize));
    coreTerms.put(OSGridTerm.gridReference.qualifiedName(), unexpectedGridReference);

    OSGridRecord osgr =
        OSGridRecord.newBuilder()
            .setId(ID)
            .setGridReference(unexpectedGridReference)
            .setGridSizeInMeters(expectedGridSize)
            .build();

    Tuple<ExtendedRecord, LocationRecord> input = createInput(er);

    OSGridInterpreter.addGridSize(input, osgr);

    // Check by reference not value
    Assert.assertSame(expectedGridSize, osgr.getGridSizeInMeters());
  }

  @Test
  public void addGrid_useProcessedGridReferenceWhenSet() {

    final String expectedGridReference = "NM39";
    final Integer expectedGridSize = 10000;

    final String unexpectedGridReference = "NH123123";
    final Integer unexpectedGridSize = 100;

    ExtendedRecord er = createTestRecord();
    Map<String, String> coreTerms = er.getCoreTerms();

    coreTerms.put(OSGridTerm.gridSizeInMeters.qualifiedName(), String.valueOf(unexpectedGridSize));
    coreTerms.put(OSGridTerm.gridReference.qualifiedName(), unexpectedGridReference);

    OSGridRecord osgr =
        OSGridRecord.newBuilder().setId(ID).setGridReference(expectedGridReference).build();

    Tuple<ExtendedRecord, LocationRecord> input = createInput(er);

    OSGridInterpreter.addGridSize(input, osgr);

    Assert.assertEquals(expectedGridSize, osgr.getGridSizeInMeters());
  }

  @Test
  public void addGrid_useSuppliedGridReferenceWhenNoProcessedGridReference() {

    final String expectedGridReference = "NM39";
    final Integer expectedGridSize = 10000;

    final Integer unexpectedGridSize = 100;

    ExtendedRecord er = createTestRecord();
    Map<String, String> coreTerms = er.getCoreTerms();

    coreTerms.put(OSGridTerm.gridSizeInMeters.qualifiedName(), String.valueOf(unexpectedGridSize));
    coreTerms.put(OSGridTerm.gridReference.qualifiedName(), expectedGridReference);

    OSGridRecord osgr = OSGridRecord.newBuilder().setId(ID).build();

    Tuple<ExtendedRecord, LocationRecord> input = createInput(er);

    OSGridInterpreter.addGridSize(input, osgr);

    Assert.assertEquals(expectedGridSize, osgr.getGridSizeInMeters());
  }

  @Test
  public void addGrid_useSuppliedGridSizeWhenNoGridReferenceSupplied() {

    final Integer expectedGridSize = 10000;

    ExtendedRecord er = createTestRecord();
    Map<String, String> coreTerms = er.getCoreTerms();

    coreTerms.put(OSGridTerm.gridSizeInMeters.qualifiedName(), String.valueOf(expectedGridSize));

    OSGridRecord osgr = OSGridRecord.newBuilder().setId(ID).build();

    Tuple<ExtendedRecord, LocationRecord> input = createInput(er);

    OSGridInterpreter.addGridSize(input, osgr);

    Assert.assertEquals(expectedGridSize, osgr.getGridSizeInMeters());
  }

  @Test
  public void validateSuppliedGridReferenceAndLatLon_issueNotAddedForCentroidLatLon() {
    ExtendedRecord er = createTestRecord();
    Map<String, String> coreTerms = er.getCoreTerms();

    String gridReference = "NM39";
    String centroidLatitude = "56.970009";
    String centroidLongitude = "-6.361995";
    String nonCentroidLongitude = "-6.777777";

    coreTerms.put(OSGridTerm.gridReference.qualifiedName(), gridReference);

    coreTerms.put(DwcTerm.decimalLongitude.qualifiedName(), centroidLongitude);
    coreTerms.put(DwcTerm.decimalLatitude.qualifiedName(), centroidLatitude);

    Tuple<ExtendedRecord, LocationRecord> input = createInput(er);

    OSGridRecord osgr = OSGridRecord.newBuilder().setId(ID).build();

    OSGridInterpreter.validateSuppliedGridReferenceAndLatLon(input, osgr);

    Assert.assertTrue(osgr.getIssues().getIssueList().isEmpty());
  }

  @Test
  public void validateSuppliedGridReferenceAndLatLon_issueAddedForNonCentroidLatLon() {
    ExtendedRecord er = createTestRecord();
    Map<String, String> coreTerms = er.getCoreTerms();

    String gridReference = "NM39";
    String centroidLatitude = "56.970009";
    String centroidLongitude = "-6.361995";
    String nonCentroidLongitude = "-6.777777";

    coreTerms.put(OSGridTerm.gridReference.qualifiedName(), gridReference);

    coreTerms.put(DwcTerm.decimalLongitude.qualifiedName(), nonCentroidLongitude);
    coreTerms.put(DwcTerm.decimalLatitude.qualifiedName(), centroidLatitude);

    Tuple<ExtendedRecord, LocationRecord> input = createInput(er);

    OSGridRecord osgr = OSGridRecord.newBuilder().setId(ID).build();

    OSGridInterpreter.validateSuppliedGridReferenceAndLatLon(input, osgr);

    Assert.assertTrue(
        osgr.getIssues()
            .getIssueList()
            .contains(NBNOccurrenceIssue.COORDINATES_NOT_CENTRE_OF_GRID.name()));
  }

  private ExtendedRecord getRecordWithAllLocationTerms(
      String gridReference,
      Integer gridSize,
      double coordinateUncertainty,
      double centroidLatitude,
      double centroidLongitude) {
    ExtendedRecord er = createTestRecord();
    Map<String, String> coreTerms = er.getCoreTerms();

    // Set all possible raw terms
    if (!Strings.isNullOrEmpty(gridReference)) {
      coreTerms.put(OSGridTerm.gridReference.qualifiedName(), gridReference);
    }

    coreTerms.put(OSGridTerm.gridSizeInMeters.qualifiedName(), gridSize.toString());

    coreTerms.put(DwcTerm.decimalLongitude.qualifiedName(), String.valueOf(centroidLongitude));
    coreTerms.put(DwcTerm.decimalLatitude.qualifiedName(), String.valueOf(centroidLatitude));
    coreTerms.put(
        DwcTerm.coordinateUncertaintyInMeters.qualifiedName(),
        String.valueOf(coordinateUncertainty));

    return er;
  }

  @Test
  public void setGridRefFromCoordinates_gridReferenceSetFromLatLon() {
    String gridReference = "NM39";
    Integer gridSize = 10000;
    double coordinateUncertainty = 7071.1;
    double centroidLatitude = 56.970009;
    double centroidLongitude = -6.361995;
    String stateProvince = "England";

    ExtendedRecord er =
        getRecordWithAllLocationTerms(
            gridReference, gridSize, coordinateUncertainty, centroidLatitude, centroidLongitude);

    LocationRecord lr =
        LocationRecord.newBuilder()
            .setId(ID)
            .setHasCoordinate(true)
            .setDecimalLatitude(centroidLatitude)
            .setDecimalLongitude(centroidLongitude)
            .setCoordinateUncertaintyInMeters(coordinateUncertainty)
            .setStateProvince(stateProvince)
            .build();

    Tuple<ExtendedRecord, LocationRecord> input = createInput(er, lr);

    OSGridRecord osgr = OSGridRecord.newBuilder().setId(ID).build();

    OSGridInterpreter.setGridRefFromCoordinates(input, osgr);

    Assert.assertEquals(gridReference, osgr.getGridReference());
  }

  @Test
  public void setGridRefFromCoordinates_gridReferenceNotSetIfNoProcessedLatLon() {
    String gridReference = "NM39";
    Integer gridSize = 10000;
    double coordinateUncertainty = 7071.1;
    double centroidLatitude = 56.970009;
    double centroidLongitude = -6.361995;
    String stateProvince = "England";

    ExtendedRecord er =
        getRecordWithAllLocationTerms(
            gridReference, gridSize, coordinateUncertainty, centroidLatitude, centroidLongitude);

    LocationRecord lr =
        LocationRecord.newBuilder()
            .setId(ID)
            .setHasCoordinate(false)
            .setCoordinateUncertaintyInMeters(coordinateUncertainty)
            .setStateProvince(stateProvince)
            .build();

    Tuple<ExtendedRecord, LocationRecord> input = createInput(er, lr);

    OSGridRecord osgr = OSGridRecord.newBuilder().setId(ID).build();

    OSGridInterpreter.setGridRefFromCoordinates(input, osgr);

    Assert.assertNull(osgr.getGridReference());
  }

  @Test
  public void setGridRefFromCoordinates_gridReferenceNotSetIfNoProcessedCoordinateUncertainty() {
    String gridReference = "NM39";
    Integer gridSize = 10000;
    double coordinateUncertainty = 7071.1;
    double centroidLatitude = 56.970009;
    double centroidLongitude = -6.361995;
    String stateProvince = "England";

    ExtendedRecord er =
        getRecordWithAllLocationTerms(
            gridReference, gridSize, coordinateUncertainty, centroidLatitude, centroidLongitude);

    LocationRecord lr =
        LocationRecord.newBuilder()
            .setId(ID)
            .setHasCoordinate(true)
            .setDecimalLatitude(centroidLatitude)
            .setDecimalLongitude(centroidLongitude)
            .setStateProvince(stateProvince)
            .build();

    Tuple<ExtendedRecord, LocationRecord> input = createInput(er, lr);

    OSGridRecord osgr = OSGridRecord.newBuilder().setId(ID).build();

    OSGridInterpreter.setGridRefFromCoordinates(input, osgr);

    Assert.assertNull(osgr.getGridReference());
  }

  @Test
  public void setGridRefFromCoordinates_gridReferenceSetFromLatLonUsingGridSizeIfSet() {
    String gridReference = "NM39";
    Integer gridSize = 10000;
    double coordinateUncertainty = 7071.1;
    double incorrectCoordinateUncertainty = 9999;
    double centroidLatitude = 56.970009;
    double centroidLongitude = -6.361995;
    String stateProvince = "England";

    ExtendedRecord er =
        getRecordWithAllLocationTerms(
            gridReference, gridSize, coordinateUncertainty, centroidLatitude, centroidLongitude);

    LocationRecord lr =
        LocationRecord.newBuilder()
            .setId(ID)
            .setHasCoordinate(true)
            .setDecimalLatitude(centroidLatitude)
            .setDecimalLongitude(centroidLongitude)
            .setCoordinateUncertaintyInMeters(incorrectCoordinateUncertainty)
            .setStateProvince(stateProvince)
            .build();

    Tuple<ExtendedRecord, LocationRecord> input = createInput(er, lr);

    OSGridRecord osgr = OSGridRecord.newBuilder().setId(ID).setGridSizeInMeters(gridSize).build();

    OSGridInterpreter.setGridRefFromCoordinates(input, osgr);

    Assert.assertEquals(gridReference, osgr.getGridReference());
  }

  @Test
  public void
      setGridRefFromCoordinates_gridReferenceSetFromLatLonUsingCoordinateUncertaintyIfNoGridSize() {
    String gridReference = "NM39";
    Integer gridSize = 10000;
    double coordinateUncertainty = 7071.1;
    double centroidLatitude = 56.970009;
    double centroidLongitude = -6.361995;
    String stateProvince = "England";

    ExtendedRecord er =
        getRecordWithAllLocationTerms(
            gridReference, gridSize, coordinateUncertainty, centroidLatitude, centroidLongitude);

    LocationRecord lr =
        LocationRecord.newBuilder()
            .setId(ID)
            .setHasCoordinate(true)
            .setDecimalLatitude(centroidLatitude)
            .setDecimalLongitude(centroidLongitude)
            .setCoordinateUncertaintyInMeters(coordinateUncertainty)
            .setStateProvince(stateProvince)
            .build();

    Tuple<ExtendedRecord, LocationRecord> input = createInput(er, lr);

    OSGridRecord osgr = OSGridRecord.newBuilder().setId(ID).build();

    OSGridInterpreter.setGridRefFromCoordinates(input, osgr);

    Assert.assertEquals(gridReference, osgr.getGridReference());
  }

  @Test
  public void setGridRefFromCoordinates_issueAddedIfRecordNotSuppliedWithGridReference() {

    String suppliedGridReference = null;
    String expectedGridReference = "NM39";

    Integer gridSize = 10000;
    double coordinateUncertainty = 7071.1;
    double centroidLatitude = 56.970009;
    double centroidLongitude = -6.361995;
    String stateProvince = "England";

    ExtendedRecord er =
        getRecordWithAllLocationTerms(
            suppliedGridReference,
            gridSize,
            coordinateUncertainty,
            centroidLatitude,
            centroidLongitude);

    LocationRecord lr =
        LocationRecord.newBuilder()
            .setId(ID)
            .setHasCoordinate(true)
            .setDecimalLatitude(centroidLatitude)
            .setDecimalLongitude(centroidLongitude)
            .setCoordinateUncertaintyInMeters(coordinateUncertainty)
            .setStateProvince(stateProvince)
            .build();

    Tuple<ExtendedRecord, LocationRecord> input = createInput(er, lr);

    OSGridRecord osgr = OSGridRecord.newBuilder().setId(ID).build();

    OSGridInterpreter.setGridRefFromCoordinates(input, osgr);

    Assert.assertEquals(expectedGridReference, osgr.getGridReference());
    Assert.assertTrue(
        osgr.getIssues()
            .getIssueList()
            .contains(NBNOccurrenceIssue.GRID_REF_CALCULATED_FROM_LAT_LONG.name()));
  }

  @Test
  public void processGridWKT_notSetIfNotSuppliedWithGridReference() {

    final String computedGridReference = "NM39";

    ExtendedRecord er = createTestRecord();
    OSGridRecord osgr =
        OSGridRecord.newBuilder().setId(ID).setGridReference(computedGridReference).build();

    Tuple<ExtendedRecord, LocationRecord> input = createInput(er);

    OSGridInterpreter.processGridWKT(input, osgr);

    Assert.assertNull(osgr.getGridReferenceWKT());
  }

  @Test
  public void processGridWKT_setFromComputedGridReferenceWhenSuppliedWithGridReference() {

    // supply different grid ref to test which one is used
    final String suppliedGridReference = "H99";
    final String computedGridReference = "SC38B";
    // expect result taken from biocache processed value
    final String expectedGridReferenceWKT =
        "POLYGON((-4.60801 54.20547,-4.60801 54.22409,-4.5785 54.22409,-4.5785 54.20547,-4.60801 54.20547))";

    ExtendedRecord er = createTestRecord();
    Map<String, String> coreTerms = er.getCoreTerms();
    coreTerms.put(OSGridTerm.gridReference.qualifiedName(), suppliedGridReference);

    OSGridRecord osgr =
        OSGridRecord.newBuilder().setId(ID).setGridReference(computedGridReference).build();

    Tuple<ExtendedRecord, LocationRecord> input = createInput(er);

    OSGridInterpreter.processGridWKT(input, osgr);

    Assert.assertEquals(expectedGridReferenceWKT, osgr.getGridReferenceWKT());
  }

  /** Not sure why this case should ever happen */
  @Test
  public void processGridWKT_setFromSuppliedGridReferenceWhenNoComputedGridReference() {

    // supply different grid ref to test which one is used
    final String suppliedGridReference = "SC38B";
    // expect result taken from biocache processed value
    final String expectedGridReferenceWKT =
        "POLYGON((-4.60801 54.20547,-4.60801 54.22409,-4.5785 54.22409,-4.5785 54.20547,-4.60801 54.20547))";

    ExtendedRecord er = createTestRecord();
    Map<String, String> coreTerms = er.getCoreTerms();
    coreTerms.put(OSGridTerm.gridReference.qualifiedName(), suppliedGridReference);

    OSGridRecord osgr = OSGridRecord.newBuilder().setId(ID).build();

    Tuple<ExtendedRecord, LocationRecord> input = createInput(er);

    OSGridInterpreter.processGridWKT(input, osgr);

    Assert.assertEquals(expectedGridReferenceWKT, osgr.getGridReferenceWKT());
  }

  @Test
  public void intTest() {
    Integer a = new Integer(100);
    Integer b = a;
    Integer c = 100;
    Integer d = 100;

    Assert.assertSame(a, b);
    Assert.assertEquals(a, c);
    Assert.assertSame(c, d);
  }
}
