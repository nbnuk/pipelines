package org.gbif.pipelines.transforms.specific;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.ABSENT_GBIF_ID_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.UNIQUE_GBIF_IDS_COUNT;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Identifier.GBIF_ID_ABSENT;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.gbif.pipelines.io.avro.GbifIdRecord;

@Slf4j
@Getter
@AllArgsConstructor(staticName = "create")
public class GbifIdTupleTransform extends PTransform<PCollection<GbifIdRecord>, PCollectionTuple> {

  private final TupleTag<GbifIdRecord> tag = new TupleTag<GbifIdRecord>() {};

  private final TupleTag<GbifIdRecord> absentTag = new TupleTag<GbifIdRecord>() {};

  @Override
  public PCollectionTuple expand(PCollection<GbifIdRecord> input) {

    // Convert from list to map where, key - occurrenceId, value - object instance and group by key
    return input.apply(
        "Filtering duplicates",
        ParDo.of(new Filter()).withOutputTags(tag, TupleTagList.of(absentTag)));
  }

  private class Filter extends DoFn<GbifIdRecord, GbifIdRecord> {

    private final Counter uniqueCounter =
        Metrics.counter(GbifIdTupleTransform.class, UNIQUE_GBIF_IDS_COUNT);
    private final Counter absentCounter =
        Metrics.counter(GbifIdTupleTransform.class, ABSENT_GBIF_ID_COUNT);

    @ProcessElement
    public void processElement(ProcessContext c) {
      GbifIdRecord gbifIdRecord = c.element();
      if (gbifIdRecord != null) {
        if (gbifIdRecord.getGbifId() == null
            && gbifIdRecord.getIssues().getIssueList().contains(GBIF_ID_ABSENT)) {
          c.output(absentTag, gbifIdRecord);
          absentCounter.inc();
        } else {
          c.output(tag, gbifIdRecord);
          uniqueCounter.inc();
        }
      }
    }
  }
}
