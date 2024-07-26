package uk.org.nbn.pipelines;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import uk.org.nbn.term.OSGridTerm;
import uk.org.nbn.util.NBNModelUtils;

public abstract class OSGridTestBase {

  protected static final String ID = "777";

  protected ExtendedRecord createTestRecord() {

    Map<String, String> coreMap = new HashMap<>();

    ExtendedRecord er =
        ExtendedRecord.newBuilder()
            .setId(ID)
            .setCoreTerms(coreMap)
            .build();

    return er;
  }
}
