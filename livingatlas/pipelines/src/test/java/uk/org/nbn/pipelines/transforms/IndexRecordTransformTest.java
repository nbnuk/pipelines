package uk.org.nbn.pipelines.transforms;

import static au.org.ala.pipelines.transforms.IndexFields.DECADE;
import static au.org.ala.pipelines.transforms.IndexFields.EVENT_DATE_END;
import static au.org.ala.pipelines.transforms.IndexRecordTransform.RAW_PREFIX;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Indexing.*;
import static org.junit.Assert.*;

import au.org.ala.pipelines.transforms.IndexRecordTransform;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Date;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.*;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import uk.org.nbn.pipelines.vocabulary.NBNOccurrenceIssue;
import uk.org.nbn.term.OSGridTerm;

public class IndexRecordTransformTest {
  private static final String ID = "777";
  private static final String UUID = "777";

  private IndexRecord getIndexRecord(TemporalRecord tr) {
    return getIndexRecord(
        ExtendedRecord.newBuilder().setId(ID).build(),
        tr,
        OSGridRecord.newBuilder().setId(ID).build());
  }

  private IndexRecord getIndexRecord(ExtendedRecord er, OSGridRecord osgr) {
    return getIndexRecord(er, TemporalRecord.newBuilder().setId(ID).build(), osgr);
  }

  private IndexRecord getIndexRecord(ExtendedRecord er, TemporalRecord tr, OSGridRecord osgr) {
    ALAUUIDRecord ur = ALAUUIDRecord.newBuilder().setId(ID).setUuid(UUID).build();
    return IndexRecordTransform.createIndexRecord(
        BasicRecord.newBuilder().setId(ID).build(),
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
    LocalDateTime endDateTime = LocalDateTime.of(eventDateEndYear, 12, 31, 23, 59, 59, 999_000_000);

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
    LocalDateTime endDateTime = LocalDateTime.of(eventDateYear, 5, 31, 23, 59, 59, 999_000_000);

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
  public void testDayRangeIndexing(String gte, String lte, int expectedDecade, int expectedYear, int expectedMonth) {

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
}
