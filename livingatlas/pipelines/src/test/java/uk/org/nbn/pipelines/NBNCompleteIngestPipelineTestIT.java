package uk.org.nbn.pipelines;

import au.org.ala.pipelines.beam.*;
import au.org.ala.pipelines.options.IndexingPipelineOptions;
import au.org.ala.pipelines.options.SamplingPipelineOptions;
import au.org.ala.pipelines.options.SolrPipelineOptions;
import au.org.ala.pipelines.options.UUIDPipelineOptions;
import au.org.ala.sampling.LayerCrawler;
import au.org.ala.util.IntegrationTestUtils;
import au.org.ala.util.SolrUtils;
import au.org.ala.utils.ValidationUtils;
import org.apache.commons.io.FileUtils;
import org.apache.parquet.Strings;
import org.apache.solr.common.SolrDocument;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.options.DwcaPipelineOptions;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import uk.org.nbn.pipelines.beam.NBNInterpretedToAccessControlledPipeline;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;


import au.com.bytecode.opencsv.CSVParser;
import au.com.bytecode.opencsv.CSVReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

/**
 * Complete pipeline tests that start with DwCAs and finish with the SOLR index. Includes all
 * current steps in processing.
 */
public class NBNCompleteIngestPipelineTestIT {

  @ClassRule public static IntegrationTestUtils itUtils = IntegrationTestUtils.getInstance();

  public static final String INDEX_NAME = "nbn_complete_occ_it";


  /** Tests for SOLR index creation. */
  @Test
  public void testIngestPipeline() throws Exception {

    // clear up previous test runs
    FileUtils.deleteQuietly(new File("/tmp/la-pipelines-test/complete-pipeline"));



    String absolutePath = new File("src/test/resources").getAbsolutePath();
    String datasetId = "dr2440";

    if(true) {
      // clear SOLR index
      SolrUtils.setupIndex(INDEX_NAME);

      // Step 1: load a dataset and verify all records have a UUID associated
      loadTestDataset("dr2440", absolutePath + "/nbn-complete-pipeline/" + datasetId);

      // reload
      SolrUtils.reloadSolrIndex(INDEX_NAME);
    }

    // validate SOLR index
    assertEquals(Long.valueOf(5), SolrUtils.getRecordCount(INDEX_NAME, "*:*"));

    // 1. includes UUIDs
    String documentId = (String) SolrUtils.getRecords(INDEX_NAME, "*:*").get(0).get("id");
    assertNotNull(documentId);
    UUID uuid = null;
    try {
      uuid = UUID.fromString(documentId);
      // do something
    } catch (IllegalArgumentException exception) {
      // handle the case where string is not valid UUID
    }

    assertNotNull(uuid);

    // 2. includes samples
    //assertEquals(Long.valueOf(5), SolrUtils.getRecordCount(INDEX_NAME, "cl620:*"));
    //assertEquals(Long.valueOf(5), SolrUtils.getRecordCount(INDEX_NAME, "cl927:*"));

    // dynamic properties indexing
//    assertEquals(
//        Long.valueOf(5),
//        SolrUtils.getRecordCount(INDEX_NAME, "dynamicProperties_nonDwcFieldSalinity:*"));

//    // 3. has a sensitive record
//    assertEquals(Long.valueOf(1), SolrUtils.getRecordCount(INDEX_NAME, "sensitive:generalised"));
//    SolrDocument sensitive = SolrUtils.getRecords(INDEX_NAME, "sensitive:generalised").get(0);
//    assertEquals(-35.3, (double) sensitive.get("decimalLatitude"), 0.00001);
//    assertEquals("-35.260319", sensitive.get("sensitive_decimalLatitude"));
//
    //4. check content of records
    checkExpectedValuesForRecords(INDEX_NAME, datasetId);
  }

  public static void checkExpectedValuesForRecords(String currentIndexName, String datasetId) throws Exception {

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

          Optional<SolrDocument> record = SolrUtils.getRecord(currentIndexName, "occurrenceID:" + nextLine[occurrenceIdHeaderIndex]);
          assertTrue(record.isPresent());

          for (int i = 0; i < header.length; i++) {
            String headerName = header[i];
            System.out.println(headerName + ": " + nextLine[i]);

            assertEqualWithTypeConversion(nextLine[i], record.get().get(headerName));
          }
          System.out.println("-----------");
        }
      }

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void assertEqualWithTypeConversion(String expected, Object actual) {

    if (expected == null) {
      Assert.assertNull(actual);
    } else {
      Assert.assertNotNull(actual);
    }

    if (actual.getClass() == expected.getClass()) {
      Assert.assertEquals(expected, actual); // Direct comparison if types match
    } else {
      // Attempt type conversion based on the type of 'actual' to match 'expected'
      if (actual instanceof Integer) {
        try {
          int expectedInt = Integer.parseInt(expected);
          Assert.assertEquals(expectedInt, actual);
        } catch (NumberFormatException e) {
          Assert.fail("Failed to convert expected String to Integer: " + expected);
        }
      } else if (actual instanceof Double) {
        try {
          double expectedInt = Double.parseDouble(expected);
          Assert.assertEquals(expectedInt, actual);
        } catch (NumberFormatException e) {
          Assert.fail("Failed to convert expected String to Double: " + expected);
        }
      } else {
        Assert.fail("Unsupported type conversion: actual=" + actual.getClass() + ", expected=" + expected.getClass());
      }
    }
  }

  public void loadTestDataset(String datasetID, String inputPath) throws Exception {

    DwcaPipelineOptions dwcaOptions =
        PipelinesOptionsFactory.create(
            DwcaPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.VERBATIM_METRICS,
              "--targetPath=/tmp/la-pipelines-test/complete-pipeline",
              "--inputPath=" + inputPath
            });
    DwcaToVerbatimPipeline.run(dwcaOptions);

    // check validation - should be false as UUIDs not generated
    assertFalse(ValidationUtils.checkValidationFile(dwcaOptions).getValid());

    ALAInterpretationPipelineOptions interpretationOptions =
        PipelinesOptionsFactory.create(
            ALAInterpretationPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--interpretationTypes=ALL",
              "--metaFileName=" + ValidationUtils.INTERPRETATION_METRICS,
              "--targetPath=/tmp/la-pipelines-test/complete-pipeline",
              "--inputPath=/tmp/la-pipelines-test/complete-pipeline/" + datasetID + "/1/verbatim.avro",
              "--properties=" + itUtils.getPropertiesFilePath(),
              "--useExtendedRecordId=true"
            });
    ALAVerbatimToInterpretedPipeline.run(interpretationOptions);

    // check validation - should be false as UUIDs not generated
    assertFalse(ValidationUtils.checkValidationFile(dwcaOptions).getValid());

    UUIDPipelineOptions uuidOptions =
        PipelinesOptionsFactory.create(
            UUIDPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.UUID_METRICS,
              "--targetPath=/tmp/la-pipelines-test/complete-pipeline",
              "--inputPath=/tmp/la-pipelines-test/complete-pipeline",
              "--properties=" + itUtils.getPropertiesFilePath(),
              "--useExtendedRecordId=true"
            });
    ALAUUIDMintingPipeline.run(uuidOptions);

    // check validation - should be true as UUIDs are validated and generated
    assertTrue(ValidationUtils.checkValidationFile(uuidOptions).getValid());

    InterpretationPipelineOptions sensitivityOptions =
        PipelinesOptionsFactory.create(
            InterpretationPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.SENSITIVE_METRICS,
              "--targetPath=/tmp/la-pipelines-test/complete-pipeline",
              "--inputPath=/tmp/la-pipelines-test/complete-pipeline",
              "--properties=" + itUtils.getPropertiesFilePath(),
              "--useExtendedRecordId=true"
            });
    ALAInterpretedToSensitivePipeline.run(sensitivityOptions);

    InterpretationPipelineOptions accessControlOptions =
            PipelinesOptionsFactory.create(
                    InterpretationPipelineOptions.class,
                    new String[] {
                            "--datasetId=" + datasetID,
                            "--attempt=1",
                            "--runner=DirectRunner",
                            "--metaFileName=" + ValidationUtils.SENSITIVE_METRICS,
                            "--targetPath=/tmp/la-pipelines-test/complete-pipeline",
                            "--inputPath=/tmp/la-pipelines-test/complete-pipeline",
                            "--properties=" + itUtils.getPropertiesFilePath(),
                            "--useExtendedRecordId=true"
                    });

    NBNInterpretedToAccessControlledPipeline.run(accessControlOptions);

    // index
    IndexingPipelineOptions solrOptions =
        PipelinesOptionsFactory.create(
            IndexingPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=SparkRunner",
              "--metaFileName=" + ValidationUtils.INDEXING_METRICS,
              "--targetPath=/tmp/la-pipelines-test/complete-pipeline",
              "--inputPath=/tmp/la-pipelines-test/complete-pipeline",
              "--allDatasetsInputPath=/tmp/la-pipelines-test/complete-pipeline/all-datasets",
              "--properties=" + itUtils.getPropertiesFilePath(),
              "--includeSensitiveDataChecks=true",
              "--includeImages=false"
            });

    // check ready for index - should be true as includeSampling=true and sampling now generated
    assertTrue(ValidationUtils.checkReadyForIndexing(solrOptions).getValid());

    IndexRecordPipeline.run(solrOptions);

    // export lat lngs
    SamplingPipelineOptions samplingOptions =
        PipelinesOptionsFactory.create(
            SamplingPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--targetPath=/tmp/la-pipelines-test/complete-pipeline",
              "--inputPath=/tmp/la-pipelines-test/complete-pipeline",
              "--allDatasetsInputPath=/tmp/la-pipelines-test/complete-pipeline/all-datasets",
              "--properties=" + itUtils.getPropertiesFilePath()
            });
    SamplingPipeline.run(samplingOptions);

    // sample
    LayerCrawler lc = new LayerCrawler();
    lc.run(samplingOptions);

    // solr
    SolrPipelineOptions solrOptions2 =
        PipelinesOptionsFactory.create(
            SolrPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=SparkRunner",
              "--metaFileName=" + ValidationUtils.INDEXING_METRICS,
              "--targetPath=/tmp/la-pipelines-test/complete-pipeline",
              "--inputPath=/tmp/la-pipelines-test/complete-pipeline",
              "--allDatasetsInputPath=/tmp/la-pipelines-test/complete-pipeline/all-datasets",
              "--properties=" + itUtils.getPropertiesFilePath(),
              "--zkHost=" + String.join(",", SolrUtils.getZkHosts()),
              "--solrCollection=" + INDEX_NAME,
              "--includeSampling=true",
              "--includeSensitiveDataChecks=true",
              "--includeImages=false",
              "--numOfPartitions=10"
            });
    IndexRecordToSolrPipeline.run(solrOptions2);
  }
}
