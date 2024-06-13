package uk.org.nbn.util;

import com.beust.jcommander.ParameterException;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.core.utils.ModelUtils;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import uk.org.nbn.term.OSGridTerm;

public class NBNModelUtils {

    public static String extractNullAwareExtensionTermValue(ExtendedRecord er, Term term) {
        if (ModelUtils.hasExtension(er, term.namespace().toString())) {
                   String value = er
                           .getExtensions()
                           .get(term.namespace().toString())
                           .get(0)
                           .getOrDefault(term.qualifiedName(), null);

                   return ModelUtils.hasValue(value) ? value : null;
        }
        return null;
    }

    public static void setExtensionTermValue(ExtendedRecord er, Term term, String value) {
        if (!ModelUtils.hasExtension(er, term.namespace().toString())) {
            throw new ParameterException("Records does not contain extension");
        }

        er.getExtensions()
                .get(OSGridTerm.gridReference.namespace().toString())
                .stream().findFirst()
                .get().put(term.qualifiedName(), value);
    }
}
