package uk.org.nbn.pipelines.java;

import au.org.ala.pipelines.beam.*;
import au.org.ala.pipelines.java.IndexRecordPipeline;
import au.org.ala.pipelines.options.IndexingPipelineOptions;
import au.org.ala.pipelines.options.SamplingPipelineOptions;
import au.org.ala.pipelines.options.SolrPipelineOptions;
import au.org.ala.pipelines.options.UUIDPipelineOptions;
import au.org.ala.sampling.LayerCrawler;
import au.org.ala.util.IntegrationTestUtils;
import au.org.ala.util.SolrUtils;
import au.org.ala.util.TestUtils;
import au.org.ala.utils.ValidationUtils;
import org.apache.commons.io.FileUtils;
import org.apache.solr.common.SolrDocument;
import org.gbif.pipelines.common.beam.options.DwcaPipelineOptions;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import uk.org.nbn.pipelines.NBNPipelineIngestTestBase;
import uk.org.nbn.pipelines.beam.NBNInterpretedToAccessControlledPipeline;

import java.io.File;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Complete pipeline tests that use the java variant of the pipeline where possible. Currently this
 * is for Interpretation and SOLR indexing only.
 *
 * <p>This needs to be ran with -Xmx128m
 * You will likely need to set the following vm options -DZK_PORT=9983 -DSOLR_PORT=8983 -DDOCKER_STARTSTOP=false
 * And also copy or link /data/pipelines-shp to /tmp/pipelines-shp
 */
public class NBNCompleteIngestJavaPipelineTestIT extends NBNPipelineIngestTestBase {

  @ClassRule public static IntegrationTestUtils itUtils = IntegrationTestUtils.getInstance();

  public static final String INDEX_NAME = "complete_java_pipeline";

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
  public Collection<DynamicTest> testIngestPipeline() throws Exception {

    Collection<DynamicTest> tests = new ArrayList<>();

    // clear up previous test runs
    FileUtils.deleteQuietly(new File("/tmp/la-pipelines-test/nbn-complete-pipeline-java"));

    String absolutePath = new File("src/test/resources").getAbsolutePath();

    Map<String,Integer> datasets = new HashMap<String,Integer>(){{
      put("dr2816", 165);
      put("dr2811", 159); //this has 4 additional records compare to ma
    }};

    int expectedRecords = datasets.values().stream().mapToInt(Integer::intValue).sum();

    //set to false in order to just running tests without reprocessing data
    if(true) {
      // clear SOLR index
      SolrUtils.setupIndex(INDEX_NAME);

        for (Map.Entry<String, Integer> entry : datasets.entrySet()) {
            String datasetId = entry.getKey();
            loadTestDataset(datasetId, absolutePath + "/nbn-complete-pipeline/" + datasetId);
        }

      // reload
      SolrUtils.reloadSolrIndex(INDEX_NAME);
    }

    // validate SOLR index
    tests.add(DynamicTest.dynamicTest("Test index", () -> {

      assertEquals("Check number of records",Long.valueOf(expectedRecords), SolrUtils.getRecordCount(INDEX_NAME, "*:*"));

      // 1. includes UUIDs
      String documentId = (String) SolrUtils.getRecords(INDEX_NAME, "*:*").get(0).get("id");
      assertNotNull("Check UUIDs present",documentId);
      UUID uuid = null;
      try {
        uuid = UUID.fromString(documentId);
        // do something
      } catch (IllegalArgumentException exception) {
        // handle the case where string is not valid UUID
      }

      assertNotNull("Check UUIDs format",uuid);
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

    // convert DwCA
    DwcaPipelineOptions dwcaOptions =
        PipelinesOptionsFactory.create(
            DwcaPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--appName=DWCA",
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.VERBATIM_METRICS,
              "--targetPath=/tmp/la-pipelines-test/nbn-complete-pipeline-java",
              "--inputPath=" + inputPath
            });
    DwcaToVerbatimPipeline.run(dwcaOptions);

    // interpret
    InterpretationPipelineOptions interpretationOptions =
        PipelinesOptionsFactory.create(
            InterpretationPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--interpretationTypes=ALL",
              "--metaFileName=" + ValidationUtils.INTERPRETATION_METRICS,
              "--targetPath=/tmp/la-pipelines-test/nbn-complete-pipeline-java",
              "--inputPath=/tmp/la-pipelines-test/nbn-complete-pipeline-java/" + datasetID + "/1/verbatim.avro",
              "--properties=" + itUtils.getPropertiesFilePath(),
              "--useExtendedRecordId=true"
            });
    au.org.ala.pipelines.java.ALAVerbatimToInterpretedPipeline.run(interpretationOptions);

    // validate and create UUIDs
    UUIDPipelineOptions uuidOptions =
        PipelinesOptionsFactory.create(
            UUIDPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.UUID_METRICS,
              "--targetPath=/tmp/la-pipelines-test/nbn-complete-pipeline-java",
              "--inputPath=/tmp/la-pipelines-test/nbn-complete-pipeline-java",
              "--properties=" + itUtils.getPropertiesFilePath(),
              "--useExtendedRecordId=true"
            });
    ALAUUIDMintingPipeline.run(uuidOptions);

    // run SDS checks
    InterpretationPipelineOptions sensitivityOptions =
        PipelinesOptionsFactory.create(
            InterpretationPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.SENSITIVE_METRICS,
              "--targetPath=/tmp/la-pipelines-test/nbn-complete-pipeline-java",
              "--inputPath=/tmp/la-pipelines-test/nbn-complete-pipeline-java",
              "--properties=" + itUtils.getPropertiesFilePath(),
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
                            "--targetPath=/tmp/la-pipelines-test/nbn-complete-pipeline-java",
                            "--inputPath=/tmp/la-pipelines-test/nbn-complete-pipeline-java",
                            "--properties=" + itUtils.getPropertiesFilePath(),
                            "--useExtendedRecordId=true"
                    });

    NBNInterpretedToAccessControlledPipeline.run(accessControlOptions);

    // index record generation
    IndexingPipelineOptions solrOptions =
        PipelinesOptionsFactory.create(
            IndexingPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--metaFileName=" + ValidationUtils.INDEXING_METRICS,
              "--targetPath=/tmp/la-pipelines-test/nbn-complete-pipeline-java",
              "--inputPath=/tmp/la-pipelines-test/nbn-complete-pipeline-java",
              "--allDatasetsInputPath=/tmp/la-pipelines-test/nbn-complete-pipeline-java/all-datasets",
              "--properties=" + itUtils.getPropertiesFilePath(),
              "--includeImages=false",
              "--includeSensitiveDataChecks=true"
            });
    IndexRecordPipeline.run(solrOptions);

    // export lat lngs
    SamplingPipelineOptions samplingOptions =
        PipelinesOptionsFactory.create(
            SamplingPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=DirectRunner",
              "--targetPath=/tmp/la-pipelines-test/nbn-complete-pipeline-java",
              "--inputPath=/tmp/la-pipelines-test/nbn-complete-pipeline-java",
              "--allDatasetsInputPath=/tmp/la-pipelines-test/nbn-complete-pipeline-java/all-datasets",
              "--properties=" + itUtils.getPropertiesFilePath()
            });
    SamplingPipeline.run(samplingOptions);

    // sample
    LayerCrawler lc = new LayerCrawler();
    lc.run(samplingOptions);

    // index into SOLR
    SolrPipelineOptions solrOptions2 =
        PipelinesOptionsFactory.create(
            SolrPipelineOptions.class,
            new String[] {
              "--datasetId=" + datasetID,
              "--attempt=1",
              "--runner=SparkRunner",
              "--metaFileName=" + ValidationUtils.INDEXING_METRICS,
              "--targetPath=/tmp/la-pipelines-test/nbn-complete-pipeline-java",
              "--inputPath=/tmp/la-pipelines-test/nbn-complete-pipeline-java",
              "--allDatasetsInputPath=/tmp/la-pipelines-test/nbn-complete-pipeline-java/all-datasets",
              "--properties=" + TestUtils.getPipelinesConfigFile(),
              "--zkHost=" + String.join(",", SolrUtils.getZkHosts()),
              "--solrCollection=" + INDEX_NAME,
              "--includeSampling=true",
              "--includeImages=false",
              "--numOfPartitions=10"
            });
    IndexRecordToSolrPipeline.run(solrOptions2);
  }
}
