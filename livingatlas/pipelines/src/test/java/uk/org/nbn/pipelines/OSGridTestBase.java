package uk.org.nbn.pipelines;

import java.util.HashMap;
import java.util.Map;
import org.gbif.pipelines.io.avro.ExtendedRecord;

public abstract class OSGridTestBase {

  protected static final String ID = "777";

  protected ExtendedRecord createTestRecord() {

    Map<String, String> coreMap = new HashMap<>();

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();

    return er;
  }
}
