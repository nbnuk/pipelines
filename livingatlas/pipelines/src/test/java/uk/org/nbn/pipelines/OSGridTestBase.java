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
    Map<String, List<Map<String, String>>> extensionsMap = new HashMap<>();
    Map<String, String> osGridMap = new HashMap<>();

    extensionsMap.put(NBNModelUtils.getExtensionNameForTerm(OSGridTerm.gridReference), Arrays.asList(osGridMap));

    ExtendedRecord er =
        ExtendedRecord.newBuilder()
            .setId(ID)
            .setCoreTerms(coreMap)
            .setExtensions(extensionsMap)
            .build();

    return er;
  }

  protected Map<String, String> getOSGridTerms(ExtendedRecord er) {
    return er.getExtensions().get(NBNModelUtils.getExtensionNameForTerm(OSGridTerm.gridReference)).get(0);
  }
}
