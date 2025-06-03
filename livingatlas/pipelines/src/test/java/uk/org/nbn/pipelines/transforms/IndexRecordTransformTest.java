package uk.org.nbn.pipelines.transforms;

import static au.org.ala.pipelines.transforms.IndexFields.EVENT_DATE_END;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Indexing.EVENT_DATE;
import static org.junit.Assert.*;

import au.org.ala.pipelines.transforms.IndexRecordTransform;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import org.gbif.pipelines.io.avro.*;
import org.junit.Test;

public class IndexRecordTransformTest {
  private static final String ID = "777";
  private static final String UUID = "777";

  private IndexRecord getIndexRecord(TemporalRecord tr) {
    ALAUUIDRecord ur = ALAUUIDRecord.newBuilder().setId(ID).setUuid(UUID).build();
    return IndexRecordTransform.createIndexRecord(
        BasicRecord.newBuilder().setId(ID).build(),
        tr,
        LocationRecord.newBuilder().setId(ID).build(),
        null,
        ALATaxonRecord.newBuilder().setId(ID).build(),
        ExtendedRecord.newBuilder().setId(ID).build(),
        ALAAttributionRecord.newBuilder().setId(ID).build(),
        ur,
        ImageRecord.newBuilder().setId(ID).build(),
        TaxonProfile.newBuilder().setId(ID).build(),
        ALASensitivityRecord.newBuilder().setId(ID).build(),
        NBNAccessControlledRecord.newBuilder().setId(ID).build(),
        OSGridRecord.newBuilder().setId(ID).build(),
        MultimediaRecord.newBuilder().setId(ID).build(),
        EventCoreRecord.newBuilder().setId(ID).build(),
        LocationRecord.newBuilder().setId(ID).build(),
        TemporalRecord.newBuilder().setId(ID).build(),
        null,
        null);
  }

  @Test
  public void testYearRangeIndexing() {

    EventDate ed = new EventDate();
    ed.setGte("2023");
    ed.setLte("2024");

    TemporalRecord tr = TemporalRecord.newBuilder().setId(ID).setEventDate(ed).build();

    IndexRecord ir = getIndexRecord(tr);

    assertTrue(ir.getDates().containsKey(EVENT_DATE));
    assertTrue(ir.getDates().containsKey(EVENT_DATE_END));

    final int eventDateYear = 2023;
    final int eventDateEndYear = 2024;

    LocalDateTime startDateTime = LocalDateTime.of(eventDateYear, 1, 1, 0, 0, 0);
    LocalDateTime endDateTime = LocalDateTime.of(eventDateEndYear, 12, 31, 23, 59, 59, 999_000_000);

    assertEquals(
        (Long) Date.from(startDateTime.toInstant(ZoneOffset.UTC)).getTime(),
        ir.getDates().get(EVENT_DATE));
    assertEquals(
        (Long) Date.from(endDateTime.toInstant(ZoneOffset.UTC)).getTime(),
        ir.getDates().get(EVENT_DATE_END));
  }

  @Test
  public void testYearMonthRangeIndexing() {

    EventDate ed = new EventDate();
    ed.setGte("2023-03");
    ed.setLte("2023-05");

    TemporalRecord tr = TemporalRecord.newBuilder().setId(ID).setEventDate(ed).build();

    IndexRecord ir = getIndexRecord(tr);

    assertTrue(ir.getDates().containsKey(EVENT_DATE));
    assertTrue(ir.getDates().containsKey(EVENT_DATE_END));

    final int eventDateYear = 2023;

    LocalDateTime startDateTime = LocalDateTime.of(eventDateYear, 3, 1, 0, 0, 0);
    LocalDateTime endDateTime = LocalDateTime.of(eventDateYear, 5, 31, 23, 59, 59, 999_000_000);

    assertEquals(
        (Long) Date.from(startDateTime.toInstant(ZoneOffset.UTC)).getTime(),
        ir.getDates().get(EVENT_DATE));
    assertEquals(
        (Long) Date.from(endDateTime.toInstant(ZoneOffset.UTC)).getTime(),
        ir.getDates().get(EVENT_DATE_END));
  }
}
