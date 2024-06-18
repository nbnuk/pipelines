package uk.org.nbn.pipelines;

import au.org.ala.pipelines.vocabulary.ALAOccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.junit.Assert;
import org.junit.Test;
import uk.org.nbn.pipelines.interpreters.NBNLocationInterpreter;

import java.util.HashMap;
import java.util.Map;

public class NBNLocationInterpreterTests extends OSGridTestBase {

    @Test
    public void issueNotAddedWhenPrecisionValueDoesNotEndInKmOrM() {

        ExtendedRecord er = createTestRecord();
        er.getCoreTerms().put(DwcTerm.coordinatePrecision.qualifiedName(), "2x");

        LocationRecord lr = LocationRecord.newBuilder().setId(ID).build();

        NBNLocationInterpreter.interpretCoordinateUncertaintyInMetersFromPrecisionFormat(er, lr);

        Assert.assertFalse(lr.getIssues().getIssueList().contains(ALAOccurrenceIssue.UNCERTAINTY_IN_PRECISION.name()));
    }

    @Test
    public void issueAddedWhenCoordinateUncertaintyInPrecision() {

        //Work around to avoid need to install the org.junit.jupiter.params.ParameterizedTest package
        Map<String,Boolean> testCases = new HashMap<>();
        testCases.put("2km", true);
        testCases.put("1km", true);
        testCases.put("2m", true);
        testCases.put("1m", false);

        testCases.forEach((key, value) -> {
            issueAddedWhenCoordinateUncertaintyInPrecisionUnless1m(key, value);
        });
    }

    public void issueAddedWhenCoordinateUncertaintyInPrecisionUnless1m(String precisionValue, boolean assertionExpected) {

        ExtendedRecord er = createTestRecord();
        er.getCoreTerms().put(DwcTerm.coordinatePrecision.qualifiedName(), precisionValue);

        LocationRecord lr = LocationRecord.newBuilder().setId(ID).build();

        NBNLocationInterpreter.interpretCoordinateUncertaintyInMetersFromPrecisionFormat(er, lr);

        Assert.assertEquals(assertionExpected, lr.getIssues().getIssueList().contains(ALAOccurrenceIssue.UNCERTAINTY_IN_PRECISION.name()));
    }
}
