package uk.org.nbn.util;

import com.beust.jcommander.ParameterException;
import java.util.Arrays;
import java.util.List;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.core.utils.ModelUtils;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import uk.org.nbn.term.OSGridTerm;

public class NBNModelUtils {

  public static String extractNullAwareExtensionTermValue(ExtendedRecord er, Term term) {
    String extensionName = getExtensionNameForTerm(term);
    if (ModelUtils.hasExtension(er, extensionName)) {
      String value =
          er.getExtensions()
              .get(extensionName)
              .get(0)
              .getOrDefault(term.qualifiedName(), null);

      return ModelUtils.hasValue(value) ? value : null;
    }
    return null;
  }

  public static void setExtensionTermValue(ExtendedRecord er, Term term, String value) {
    if (!ModelUtils.hasExtension(er, getExtensionNameForTerm(term))) {
      throw new ParameterException("Records does not contain extension");
    }

    er.getExtensions().get(getExtensionNameForTerm(OSGridTerm.gridReference)).stream()
        .findFirst()
        .get()
        .put(term.qualifiedName(), value);
  }

  private static String getExtensionNameForTerm(Term input) {
    return removeTrailingSlash(input.namespace().toString());
  }

  private static String removeTrailingSlash(String input) {
    if(input.endsWith("/")) {
      return  input.substring(0, input.length() - 1);
    } else {
      return  input;
    }
  }

  private static String listSeperator = "[,|]";

  public static List<String> getListFromString(String input) {
    return Arrays.asList(input.split(listSeperator));
  }

  public static String getStringFromList(List<String> input) {
    return String.join("|", input);
  }
}
