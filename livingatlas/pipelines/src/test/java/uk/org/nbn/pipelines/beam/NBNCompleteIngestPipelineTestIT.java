package uk.org.nbn.pipelines.beam;

import au.org.ala.kvs.GeocodeShpConfig;
import au.org.ala.kvs.ShapeFile;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import uk.org.nbn.pipelines.NBNPipelineIngestTestBase;
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
 * You will likely need to set the following vm options -DZK_PORT=9983 -DSOLR_PORT=8983 -DDOCKER_STARTSTOP=false
 * And also copy or link /data/pipelines-shp to /tmp/pipelines-shp
 */
public class NBNCompleteIngestPipelineTestIT extends NBNPipelineIngestTestBase {

  @ClassRule public static IntegrationTestUtils itUtils = IntegrationTestUtils.getInstance();

  public static final String INDEX_NAME = "nbn_complete_occ_it";


  @BeforeAll
  public static void beforeAll() throws Throwable
  {
    itUtils.before();
  }

  @AfterAll
  public static void afterAll()
  {
    itUtils.after();
  }

  /** Tests for SOLR index creation. */
  @TestFactory
  public Collection<DynamicTest> testIngestPipeline() throws Exception,Throwable {

    //need to copy /data/pipelines-shp to /tmp/pipelines-shp

    Collection<DynamicTest> tests = new ArrayList<>();

    // clear up previous test runs
    FileUtils.deleteQuietly(new File("/tmp/la-pipelines-test/nbn-complete-pipeline"));

    String absolutePath = new File("src/test/resources").getAbsolutePath();

    Map<String,Integer> datasets = new HashMap<String,Integer>(){{
      put("dr2816", 165);
      put("dr2811", 155);
    }};

    int expectedRecords = datasets.values().stream().mapToInt(Integer::intValue).sum();

    //set to false in order to just running tests without reprocessing data
    if(true) {
      // clear SOLR index
      SolrUtils.setupIndex(INDEX_NAME);

      // Step 1: load a dataset and verify all records have a UUID associated
      for (Map.Entry<String, Integer> entry : datasets.entrySet()) {
        String datasetId = entry.getKey();
        loadTestDataset(datasetId, absolutePath + "/nbn-complete-pipeline/" + datasetId);
      }

      // reload
      SolrUtils.reloadSolrIndex(INDEX_NAME);
    }

    // validate SOLR index
    tests.add(DynamicTest.dynamicTest("Test index", () -> {

      assertEquals(Long.valueOf(expectedRecords), SolrUtils.getRecordCount(INDEX_NAME, "*:*"));

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
    }));

    //4. check content of records
    for (Map.Entry<String, Integer> entry : datasets.entrySet()) {
      String datasetId = entry.getKey();
      Collection<DynamicTest> occurrenceTests = checkExpectedValuesForRecords(INDEX_NAME, datasetId);
      tests.addAll(occurrenceTests);
    }

    return tests;
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
              "--targetPath=/tmp/la-pipelines-test/nbn-complete-pipeline",
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
              "--targetPath=/tmp/la-pipelines-test/nbn-complete-pipeline",
              "--inputPath=/tmp/la-pipelines-test/nbn-complete-pipeline/" + datasetID + "/1/verbatim.avro",
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
              "--targetPath=/tmp/la-pipelines-test/nbn-complete-pipeline",
              "--inputPath=/tmp/la-pipelines-test/nbn-complete-pipeline",
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
              "--targetPath=/tmp/la-pipelines-test/nbn-complete-pipeline",
              "--inputPath=/tmp/la-pipelines-test/nbn-complete-pipeline",
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
                            "--targetPath=/tmp/la-pipelines-test/nbn-complete-pipeline",
                            "--inputPath=/tmp/la-pipelines-test/nbn-complete-pipeline",
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
              "--targetPath=/tmp/la-pipelines-test/nbn-complete-pipeline",
              "--inputPath=/tmp/la-pipelines-test/nbn-complete-pipeline",
              "--allDatasetsInputPath=/tmp/la-pipelines-test/nbn-complete-pipeline/all-datasets",
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
              "--targetPath=/tmp/la-pipelines-test/nbn-complete-pipeline",
              "--inputPath=/tmp/la-pipelines-test/nbn-complete-pipeline",
              "--allDatasetsInputPath=/tmp/la-pipelines-test/nbn-complete-pipeline/all-datasets",
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
              "--targetPath=/tmp/la-pipelines-test/nbn-complete-pipeline",
              "--inputPath=/tmp/la-pipelines-test/nbn-complete-pipeline",
              "--allDatasetsInputPath=/tmp/la-pipelines-test/nbn-complete-pipeline/all-datasets",
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
