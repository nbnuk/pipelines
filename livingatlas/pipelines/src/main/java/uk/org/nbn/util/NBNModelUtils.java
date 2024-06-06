package uk.org.nbn.util;

import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.core.utils.ModelUtils;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class NBNModelUtils {

    public static String extractNullAwareExtensionTerm(ExtendedRecord er, Term term) {
        if (ModelUtils.hasExtension(er, term.namespace().toString())) {
                   return er
                           .getExtensions()
                           .get(term.namespace().toString())
                           .get(0)
                           .getOrDefault(term.qualifiedName(), null);
        }
        return null;
    }
}
