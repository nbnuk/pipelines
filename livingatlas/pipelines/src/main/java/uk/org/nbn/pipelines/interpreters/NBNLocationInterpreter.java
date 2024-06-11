package uk.org.nbn.pipelines.interpreters;

import au.org.ala.pipelines.vocabulary.ALAOccurrenceIssue;
import com.google.common.base.Strings;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.spark_project.guava.primitives.Doubles;
import sun.misc.FloatingDecimal;
import org.gbif.common.parsers.NumberParser;

import static org.gbif.pipelines.core.utils.ModelUtils.addIssue;
import static org.gbif.pipelines.core.utils.ModelUtils.extractNullAwareValue;

public class NBNLocationInterpreter {
    public static void interpretCoordinateUncertaintyInMeters(ExtendedRecord er, LocationRecord lr) {
        String uncertaintyValue = extractNullAwareValue(er, DwcTerm.coordinateUncertaintyInMeters);
        String precisionValue = extractNullAwareValue(er, DwcTerm.coordinatePrecision);

        //check to see if the uncertainty has incorrectly been put in the precision
        //another way to misuse coordinatePrecision
        if (!Strings.isNullOrEmpty(precisionValue)) {

            if (precisionValue.endsWith("km") || precisionValue.endsWith("m")) {
                Double precision = null;
                if (precisionValue.endsWith("km")) {
                    Double precisionResult = Doubles.tryParse(precisionValue.substring(0, precisionValue.length() - "km".length()));
                    // multiply result to meters
                    if (precisionResult != null && precisionResult > 0) precision = precisionResult * 1000;

                } else {
                    precision = NumberParser.parseDouble(precisionValue.substring(0, precisionValue.length() - "m".length()));

                }

                if (precision != null && precision > 1) {
                    lr.setCoordinateUncertaintyInMeters(precision);

                    String comment = "Supplied precision, " + precisionValue + ", is assumed to be uncertainty in metres";
                    addIssue(lr, ALAOccurrenceIssue.UNCERTAINTY_IN_PRECISION.name());

                    // Presumably this is removed as it is implied by the above issue
                    lr.getIssues().getIssueList().remove(OccurrenceIssue.COORDINATE_PRECISION_INVALID.name());
                }
            }
        }


    }
}
