package uk.org.nbn.util;

import com.beust.jcommander.ParameterException;
import java.util.Arrays;
import java.util.List;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.core.utils.ModelUtils;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import uk.org.nbn.term.OSGridTerm;

public class NBNModelUtils {

  public static void setTermValue(ExtendedRecord er, Term term, String value) {
    er.getCoreTerms().put(term.qualifiedName(), value);
  }

  private static String listSeperator = "[,|]";

  public static List<String> getListFromString(String input) {
    return Arrays.asList(input.split(listSeperator));
  }

  public static String getStringFromList(List<String> input) {
    return String.join("|", input);
  }
}
