package uk.org.nbn.parser;

import java.util.Map;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.core.parsers.common.ParsedField;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Assert;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import uk.org.nbn.pipelines.OSGridTestBase;
import uk.org.nbn.term.OSGridTerm;

public class OSGridParserTests extends OSGridTestBase {

  // Values taken from GridReferenceTest.scala
  @ParameterizedTest
  @CsvSource({"NM39, 56.970009, -6.361995", "H99, 54.793876, -6.523798"})
  public void parseFromGridReference(
      String gridReference, Double expectedLatitude, Double expectedLongitude) {
    ExtendedRecord er = createTestRecord();
    Map<String, String> osGridMap = getOSGridTerms(er);

    osGridMap.put(OSGridTerm.gridReference.qualifiedName(), gridReference);

    ParsedField<LatLng> result = OSGridParser.parseCoords(er);

    Assert.assertTrue(result.isSuccessful());
    Assert.assertEquals(expectedLatitude, result.getResult().getLatitude());
    Assert.assertEquals(expectedLongitude, result.getResult().getLongitude());
  }
}
