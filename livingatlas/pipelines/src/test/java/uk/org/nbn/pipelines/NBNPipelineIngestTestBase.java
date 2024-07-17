package uk.org.nbn.pipelines;

import au.com.bytecode.opencsv.CSVParser;
import au.com.bytecode.opencsv.CSVReader;
import au.org.ala.util.SolrUtils;
import org.apache.commons.io.FileUtils;
import org.apache.parquet.Strings;
import org.apache.solr.common.SolrDocument;
import org.junit.Assert;
import org.junit.jupiter.api.DynamicTest;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.junit.Assert.assertTrue;

public class NBNPipelineIngestTestBase {
    protected static Collection<DynamicTest> checkExpectedValuesForRecords(String currentIndexName, String datasetId) throws Exception {
        Collection<DynamicTest> tests = new ArrayList<>();

        try (InputStream inputStream = FileUtils.openInputStream(
                new File("src/test/resources/nbn-complete-pipeline/expected/" + datasetId + ".csv"));
             InputStreamReader reader = new InputStreamReader(inputStream);
             CSVReader csvReader = new CSVReader(reader, CSVParser.DEFAULT_SEPARATOR,
                     CSVParser.DEFAULT_QUOTE_CHARACTER,
                     CSVParser.DEFAULT_ESCAPE_CHARACTER,
                     0,
                     CSVParser.DEFAULT_STRICT_QUOTES,
                     CSVParser.DEFAULT_IGNORE_LEADING_WHITESPACE)) {

            String[] header = csvReader.readNext(); // Read the header
            if (header != null) {

                int occurrenceIdHeaderIndex = IntStream.range(0, header.length)
                        .filter(i -> "occurrenceID".equals(header[i]))
                        .findFirst().getAsInt();

                String[] nextLine;
                while ((nextLine = csvReader.readNext()) != null) {

                    final String[] expectedValues = nextLine;
                    final String occurrenceId = expectedValues[occurrenceIdHeaderIndex];
                    tests.add(DynamicTest.dynamicTest("Validate occurrence " + occurrenceId, () -> {

                        Optional<SolrDocument> record = SolrUtils.getRecord(currentIndexName, "occurrenceID:" + expectedValues[occurrenceIdHeaderIndex].replace(":", "\\:"));
                        assertTrue("The record is not present in solr", record.isPresent());

                        for (int i = 0; i < header.length; i++) {
                            String headerName = header[i];
                            System.out.println(headerName + ": " + expectedValues[i]);

                            assertEqualWithTypeConversion(headerName, expectedValues[i], record.get().get(headerName));
                        }
                    }));
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return tests;
    }

    private static void assertEqualWithTypeConversion(String name, String expected, Object actual) {

        if (Strings.isNullOrEmpty(expected)) {
            Assert.assertNull("The property was not null: " + name, actual);
            return;
        }

        Assert.assertNotNull("The property was null: " + name, actual);

        if (actual.getClass() == expected.getClass()) {
            Assert.assertEquals(expected, actual); // Direct comparison if types match
        } else {
            // Attempt type conversion based on the type of 'actual' to match 'expected'
            if (actual instanceof Integer) {
                try {
                    int expectedInt = Integer.parseInt(expected);
                    Assert.assertEquals("The property did not match: " + name, expectedInt, actual);
                } catch (NumberFormatException e) {
                    Assert.fail("Failed to convert expected String to Integer: " + name + " " + expected);
                }
            } else if (actual instanceof Double) {
                try {
                    double expectedInt = Double.parseDouble(expected);
                    //Cannot use Assert.assertEquals as fails when comparing -0 and 0
                    Assert.assertTrue("The property did not match: " + name,expectedInt == (double) actual);
                } catch (NumberFormatException e) {
                    Assert.fail("Failed to convert expected String to Double: " + name + " " + expected);
                }
            } else {
                Assert.fail("Unsupported type conversion: actual=" + actual.getClass() + ", expected=" + expected.getClass());
            }
        }
    }

}
