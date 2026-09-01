package uk.org.nbn.pipelines.transforms;

import static au.org.ala.pipelines.transforms.IndexFields.*;
import static au.org.ala.pipelines.transforms.IndexRecordTransform.RAW_PREFIX;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Indexing.*;
import static org.junit.Assert.*;

import au.org.ala.pipelines.transforms.IndexRecordTransform;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.*;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import uk.org.nbn.pipelines.vocabulary.NBNOccurrenceIssue;
import uk.org.nbn.term.OSGridTerm;

public class IndexRecordTransformTest {
  private static final String ID = "777";
  private static final String UUID = "777";

  private IndexRecord getIndexRecord(ExtendedRecord er, BasicRecord br) {
    return getIndexRecord(
        er,
        TemporalRecord.newBuilder().setId(ID).build(),
        OSGridRecord.newBuilder().setId(ID).build(),
        br);
  }

  private IndexRecord getIndexRecord(TemporalRecord tr) {
    return getIndexRecord(
        ExtendedRecord.newBuilder().setId(ID).build(),
        tr,
        OSGridRecord.newBuilder().setId(ID).build(),
        BasicRecord.newBuilder().setId(ID).build());
  }

  private IndexRecord getIndexRecord(ExtendedRecord er, OSGridRecord osgr) {
    return getIndexRecord(
        er,
        TemporalRecord.newBuilder().setId(ID).build(),
        osgr,
        BasicRecord.newBuilder().setId(ID).build());
  }

  private IndexRecord getIndexRecord(
      ExtendedRecord er, TemporalRecord tr, OSGridRecord osgr, BasicRecord br) {
    ALAUUIDRecord ur = ALAUUIDRecord.newBuilder().setId(ID).setUuid(UUID).build();
    return IndexRecordTransform.createIndexRecord(
        br,
        tr,
        LocationRecord.newBuilder().setId(ID).build(),
        null,
        ALATaxonRecord.newBuilder().setId(ID).build(),
        er,
        ALAAttributionRecord.newBuilder().setId(ID).build(),
        ur,
        ImageRecord.newBuilder().setId(ID).build(),
        TaxonProfile.newBuilder().setId(ID).build(),
        ALASensitivityRecord.newBuilder().setId(ID).build(),
        NBNAccessControlledRecord.newBuilder().setId(ID).build(),
        osgr,
        MultimediaRecord.newBuilder().setId(ID).build(),
        EventCoreRecord.newBuilder().setId(ID).build(),
        LocationRecord.newBuilder().setId(ID).build(),
        TemporalRecord.newBuilder().setId(ID).build(),
        null,
        null);
  }

  @Test
  public void
      givenLatLonComputedFromGridRefAndNoGeodeticDatum_whenIndexing_shouldRemoveRawLatLonAndGeodeticDatum() {

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).build();

    er.getCoreTerms().put(DwcTerm.decimalLatitude.qualifiedName(), "0");
    er.getCoreTerms().put(DwcTerm.decimalLongitude.qualifiedName(), "0");
    er.getCoreTerms().put(DwcTerm.geodeticDatum.qualifiedName(), "ESPG:4326");

    OSGridRecord osgr =
        OSGridRecord.newBuilder()
            .setId(ID)
            .setGridReference("AA")
            .setGridSizeInMeters(10000)
            .setIssuesBuilder(
                IssueRecord.newBuilder()
                    .setIssueList(
                        Arrays.asList(
                            NBNOccurrenceIssue.DECIMAL_LAT_LONG_CALCULATED_FROM_GRID_REF.name())))
            .build();

    IndexRecord ir = getIndexRecord(er, osgr);

    assertFalse(ir.getStrings().containsKey(RAW_PREFIX + DECIMAL_LATITUDE));
    assertFalse(ir.getStrings().containsKey(RAW_PREFIX + DECIMAL_LONGITUDE));
    assertFalse(ir.getStrings().containsKey(RAW_PREFIX + DwcTerm.geodeticDatum.simpleName()));
  }

  @Test
  public void
      givenLatLonComputedFromGridRefAndGeodeticDatumSupplied_whenIndexing_shouldRemoveRawLatLonAndSuppliedGeodeticDatumSetAsRaw() {

    final String OSGB_GEODETIC_DATUM = "OSGB";

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).build();

    er.getCoreTerms().put(DwcTerm.decimalLatitude.qualifiedName(), "0");
    er.getCoreTerms().put(DwcTerm.decimalLongitude.qualifiedName(), "0");
    er.getCoreTerms()
        .put(OSGridTerm.gridReferenceGeodeticDatum.qualifiedName(), OSGB_GEODETIC_DATUM);

    OSGridRecord osgr =
        OSGridRecord.newBuilder()
            .setId(ID)
            .setGridReference("AA")
            .setGridSizeInMeters(10000)
            .setIssuesBuilder(
                IssueRecord.newBuilder()
                    .setIssueList(
                        Arrays.asList(
                            NBNOccurrenceIssue.DECIMAL_LAT_LONG_CALCULATED_FROM_GRID_REF.name())))
            .build();

    IndexRecord ir = getIndexRecord(er, osgr);

    assertFalse(ir.getStrings().containsKey(RAW_PREFIX + DECIMAL_LATITUDE));
    assertFalse(ir.getStrings().containsKey(RAW_PREFIX + DECIMAL_LONGITUDE));
    assertTrue(ir.getStrings().containsKey(RAW_PREFIX + DwcTerm.geodeticDatum.simpleName()));
    assertEquals(
        OSGB_GEODETIC_DATUM, ir.getStrings().get(RAW_PREFIX + DwcTerm.geodeticDatum.simpleName()));
  }

  // inputs taken from GridUtil tests
  @ParameterizedTest
  @CsvSource({
    "NH123123, NH, NH11, -, NH1212, NH123123",
    "NH12341234, NH, NH11, -, NH1212, NH123123",
    "NH1234512345, NH, NH11, NH11G, NH1212, NH123123",
    "J12341234, J, J11, -, J1212, J123123",
    "J43214321, J, J44, J44G, J4343, J432432"
  })
  public void givenAGridReference_whenIndexing_resolutionsShouldBeAdded(
      String gridReference,
      String grid_ref_100000,
      String grid_ref_10000,
      String grid_ref_2000,
      String grid_ref_1000,
      String grid_ref_100) {
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).build();

    OSGridRecord osgr = OSGridRecord.newBuilder().setId(ID).setGridReference(gridReference).build();

    IndexRecord ir = getIndexRecord(er, osgr);

    Assert.assertEquals(grid_ref_100000, ir.getStrings().get("grid_ref_100000"));
    Assert.assertEquals(grid_ref_10000, ir.getStrings().get("grid_ref_10000"));
    Assert.assertEquals(grid_ref_1000, ir.getStrings().get("grid_ref_1000"));
    Assert.assertEquals(grid_ref_100, ir.getStrings().get("grid_ref_100"));

    if (!grid_ref_2000.equals("-")) {
      Assert.assertEquals(grid_ref_2000, ir.getStrings().get("grid_ref_2000"));
    }
  }

  @Test
  public void givenAGridReferenceThatFailsToGeneralise_whenIndexing_shouldNotThrow() {
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).build();

    OSGridRecord osgr =
        OSGridRecord.newBuilder()
            .setId(ID)
            .setGridReference("K0156")
            .setGridSizeInMeters(1000)
            .build();

    IndexRecord ir = getIndexRecord(er, osgr);

    assertFalse(ir.getStrings().containsKey("grid_ref_10000"));
  }

  @Test
  public void testYearIndexing() {

    EventDate ed = new EventDate();
    ed.setGte("2023");

    TemporalRecord tr =
        TemporalRecord.newBuilder().setId(ID).setEventDate(ed).setDatePrecision("YEAR").build();

    IndexRecord ir = getIndexRecord(tr);

    assertFalse(ir.getDates().containsKey(EVENT_DATE));
    assertFalse(ir.getDates().containsKey(EVENT_DATE_END));
  }

  @Test
  public void testMonthIndexing() {

    EventDate ed = new EventDate();
    ed.setGte("2023-03");

    TemporalRecord tr =
        TemporalRecord.newBuilder().setId(ID).setEventDate(ed).setDatePrecision("MONTH").build();

    IndexRecord ir = getIndexRecord(tr);

    assertFalse(ir.getDates().containsKey(EVENT_DATE));
    assertFalse(ir.getDates().containsKey(EVENT_DATE_END));
  }

  @Test
  public void testYearRangeIndexing() {

    EventDate ed = new EventDate();
    ed.setGte("2023");
    ed.setLte("2024");

    TemporalRecord tr =
        TemporalRecord.newBuilder()
            .setId(ID)
            .setEventDate(ed)
            .setDatePrecision("YEAR_RANGE")
            .build();

    IndexRecord ir = getIndexRecord(tr);

    assertTrue(ir.getDates().containsKey(EVENT_DATE));
    assertTrue(ir.getDates().containsKey(EVENT_DATE_END));
    assertTrue(ir.getInts().containsKey(YEAR));
    assertTrue(ir.getInts().containsKey(DECADE));

    final int eventDateYear = 2023;
    final int eventDateDecade = 2020;
    final int eventDateEndYear = 2024;

    LocalDateTime startDateTime = LocalDateTime.of(eventDateYear, 1, 1, 0, 0, 0);
    LocalDateTime endDateTime = LocalDateTime.of(eventDateEndYear, 12, 31, 0, 0, 0);

    assertEquals(
        (Long) Date.from(startDateTime.toInstant(ZoneOffset.UTC)).getTime(),
        ir.getDates().get(EVENT_DATE));
    assertEquals(
        (Long) Date.from(endDateTime.toInstant(ZoneOffset.UTC)).getTime(),
        ir.getDates().get(EVENT_DATE_END));
    assertEquals(eventDateYear, (int) ir.getInts().get(YEAR));
    assertEquals(eventDateDecade, (int) ir.getInts().get(DECADE));
  }

  @Test
  public void testYearMonthRangeIndexing() {

    EventDate ed = new EventDate();
    ed.setGte("2023-03");
    ed.setLte("2023-05");

    TemporalRecord tr =
        TemporalRecord.newBuilder()
            .setId(ID)
            .setEventDate(ed)
            .setDatePrecision("MONTH_RANGE")
            .build();

    IndexRecord ir = getIndexRecord(tr);

    assertTrue(ir.getDates().containsKey(EVENT_DATE));
    assertTrue(ir.getDates().containsKey(EVENT_DATE_END));
    assertTrue(ir.getInts().containsKey(YEAR));
    assertTrue(ir.getInts().containsKey(MONTH));
    assertTrue(ir.getInts().containsKey(DECADE));

    final int eventDateYear = 2023;
    final int eventDateDecade = 2020;
    final int eventDateMonth = 3;

    LocalDateTime startDateTime = LocalDateTime.of(eventDateYear, eventDateMonth, 1, 0, 0, 0);
    LocalDateTime endDateTime = LocalDateTime.of(eventDateYear, 5, 31, 0, 0, 0);

    assertEquals(
        (Long) Date.from(startDateTime.toInstant(ZoneOffset.UTC)).getTime(),
        ir.getDates().get(EVENT_DATE));
    assertEquals(
        (Long) Date.from(endDateTime.toInstant(ZoneOffset.UTC)).getTime(),
        ir.getDates().get(EVENT_DATE_END));
    assertEquals(eventDateYear, (int) ir.getInts().get(YEAR));
    assertEquals(eventDateMonth, (int) ir.getInts().get(MONTH));
    assertEquals(eventDateDecade, (int) ir.getInts().get(DECADE));
  }

  @ParameterizedTest
  @CsvSource({
    "2023-01-01, 2024-05-31, 2020, 2023, 1",
    "2007-05-08, 2007-06-08, 2000, 2007, 5",
  })
  public void testDayRangeIndexing(
      String gte, String lte, int expectedDecade, int expectedYear, int expectedMonth) {

    EventDate ed = new EventDate();
    ed.setGte(gte);
    ed.setLte(lte);

    TemporalRecord tr =
        TemporalRecord.newBuilder()
            .setId(ID)
            .setEventDate(ed)
            .setDatePrecision("DAY_RANGE")
            .build();

    IndexRecord ir = getIndexRecord(tr);

    assertTrue(ir.getInts().containsKey(YEAR));
    assertTrue(ir.getInts().containsKey(MONTH));
    assertTrue(ir.getInts().containsKey(DECADE));

    assertEquals(expectedYear, (int) ir.getInts().get(YEAR));
    assertEquals(expectedMonth, (int) ir.getInts().get(MONTH));
    assertEquals(expectedDecade, (int) ir.getInts().get(DECADE));
  }

  @Test
  public void testSecondsTimestamp() {
    long tsSeconds = 1609459200L; // 2021-01-01 in seconds
    long expectedMillis = 1609459200000L;

    IndexRecord ir = IndexRecord.newBuilder().setId(ID).build();
    ir.getDates().put(FIRST_LOADED_DATE, tsSeconds);
    IndexRecordTransform.ensureFirstLoadedDateTimestampMilliseconds(ir);

    assertEquals(expectedMillis, (long) ir.getDates().get(FIRST_LOADED_DATE));
  }

  @Test
  public void testMillisecondsTimestamp() {
    long tsMillis = 1609459200000L; // 2021-01-01 in milliseconds
    long expectedMillis = 1609459200000L;

    IndexRecord ir = IndexRecord.newBuilder().setId(ID).build();
    ir.getDates().put(FIRST_LOADED_DATE, tsMillis);
    IndexRecordTransform.ensureFirstLoadedDateTimestampMilliseconds(ir);

    assertEquals(expectedMillis, (long) ir.getDates().get(FIRST_LOADED_DATE));
  }

  @Test
  public void testRecentSecondsTimestamp() {
    long tsSeconds = 1700000000L; // ~2023 in seconds
    long expectedMillis = 1700000000000L;

    IndexRecord ir = IndexRecord.newBuilder().setId(ID).build();
    ir.getDates().put(FIRST_LOADED_DATE, tsSeconds);
    IndexRecordTransform.ensureFirstLoadedDateTimestampMilliseconds(ir);

    assertEquals(expectedMillis, (long) ir.getDates().get(FIRST_LOADED_DATE));
  }

  @Test
  public void testRecentMillisecondsTimestamp() {
    long tsMillis = 1700000000000L; // ~2023 in milliseconds
    long expectedMillis = 1700000000000L;

    IndexRecord ir = IndexRecord.newBuilder().setId(ID).build();
    ir.getDates().put(FIRST_LOADED_DATE, tsMillis);
    IndexRecordTransform.ensureFirstLoadedDateTimestampMilliseconds(ir);

    assertEquals(expectedMillis, (long) ir.getDates().get(FIRST_LOADED_DATE));
  }

  @Test
  public void givenAParsableLifeStage_whenIndexing_rawLifeStageShouldBeUsed() {
    final String expected = "obscure lifestage variant";

    BasicRecord br =
        BasicRecord.newBuilder()
            .setLifeStage(
                VocabularyConcept.newBuilder()
                    .setConcept("Adult")
                    .setLineage(Collections.emptyList())
                    .build())
            .setId(ID)
            .build();

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).build();
    er.getCoreTerms().put(DwcTerm.lifeStage.qualifiedName(), expected);

    IndexRecord ir = getIndexRecord(er, br);

    Assert.assertNull(ir.getStrings().get(RAW_PREFIX + DwcTerm.lifeStage.simpleName()));
    Assert.assertEquals(expected, ir.getStrings().get(DwcTerm.lifeStage.simpleName()));
  }

  @Test
  public void givenAnUnparsableLifeStage_whenIndexing_rawLifeStageShouldBeUsed() {
    final String expected = "obscure lifestage variant";

    BasicRecord br =
        BasicRecord.newBuilder()
            .setLifeStage(
                VocabularyConcept.newBuilder()
                    .setConcept("xxxxxx")
                    .setLineage(Collections.emptyList())
                    .build())
            .setId(ID)
            .build();

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).build();
    er.getCoreTerms().put(DwcTerm.lifeStage.qualifiedName(), expected);

    IndexRecord ir = getIndexRecord(er, br);

    Assert.assertNull(ir.getStrings().get(RAW_PREFIX + DwcTerm.lifeStage.simpleName()));
    Assert.assertEquals(expected, ir.getStrings().get(DwcTerm.lifeStage.simpleName()));
  }

  @Test
  public void givenNoLifeStage_whenIndexing_lifeStageShouldBeNull() {
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).build();

    IndexRecord ir = getIndexRecord(er, br);

    Assert.assertNull(ir.getStrings().get(RAW_PREFIX + DwcTerm.lifeStage.simpleName()));
    Assert.assertNull(ir.getStrings().get(DwcTerm.lifeStage.simpleName()));
  }

  @Test
  public void givenAParsableSex_whenIndexing_rawSexShouldBeUsed() {
    final String expected = "obscure sex variant";

    BasicRecord br = BasicRecord.newBuilder().setSex("Male").setId(ID).build();

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).build();
    er.getCoreTerms().put(DwcTerm.sex.qualifiedName(), expected);

    IndexRecord ir = getIndexRecord(er, br);

    Assert.assertNull(ir.getStrings().get(RAW_PREFIX + DwcTerm.sex.simpleName()));
    Assert.assertEquals(expected, ir.getStrings().get(DwcTerm.sex.simpleName()));
  }

  @Test
  public void givenAnUnparsableSex_whenIndexing_rawSexShouldBeUsed() {
    final String expected = "obscure sex variant";

    BasicRecord br = BasicRecord.newBuilder().setSex("xxxxxx").setId(ID).build();

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).build();
    er.getCoreTerms().put(DwcTerm.sex.qualifiedName(), expected);

    IndexRecord ir = getIndexRecord(er, br);

    Assert.assertNull(ir.getStrings().get(RAW_PREFIX + DwcTerm.sex.simpleName()));
    Assert.assertEquals(expected, ir.getStrings().get(DwcTerm.sex.simpleName()));
  }

  @Test
  public void givenNoSex_whenIndexing_SexShouldBeNull() {
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).build();

    IndexRecord ir = getIndexRecord(er, br);

    Assert.assertNull(ir.getStrings().get(RAW_PREFIX + DwcTerm.sex.simpleName()));
    Assert.assertNull(ir.getStrings().get(DwcTerm.sex.simpleName()));
  }
}
