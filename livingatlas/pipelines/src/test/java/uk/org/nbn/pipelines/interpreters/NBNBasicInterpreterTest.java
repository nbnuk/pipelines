package uk.org.nbn.pipelines.interpreters;

import static org.junit.Assert.*;

import au.org.ala.pipelines.vocabulary.Vocab;
import au.org.ala.util.TestUtils;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.OccurrenceStatus;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.core.interpreters.core.BasicInterpreter;
import org.gbif.pipelines.factory.OccurrenceStatusKvStoreFactory;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Test;
import uk.org.nbn.pipelines.vocabulary.IdentificationVerificationStatus;
import uk.org.nbn.pipelines.vocabulary.NBNLicense;
import uk.org.nbn.pipelines.vocabulary.NBNOccurrenceIssue;

public class NBNBasicInterpreterTest {
  private static final String ID = "777";

  @Test
  public void interpretLicenseEmptyTest() throws FileNotFoundException {
    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DcTerm.license.qualifiedName(), "");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();
    Vocab vocab = NBNLicense.getInstance(null);

    // When
    BiConsumer<ExtendedRecord, BasicRecord> consumer = NBNBasicInterpreter.interpretLicense(vocab);
    consumer.accept(er, br);

    // Should
    assertEquals("UNSPECIFIED", br.getLicense());
    assertTrue(br.getIssues().getIssueList().isEmpty());
  }

  @Test
  public void interpretLicenseNullTest() throws FileNotFoundException {
    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DcTerm.license.qualifiedName(), null);
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();
    Vocab vocab = NBNLicense.getInstance(null);

    // When
    BiConsumer<ExtendedRecord, BasicRecord> consumer = NBNBasicInterpreter.interpretLicense(vocab);
    consumer.accept(er, br);

    // Should
    assertEquals("UNSPECIFIED", br.getLicense());
    assertTrue(br.getIssues().getIssueList().isEmpty());
  }

  @Test
  public void interpretLicenseMatchingTest() throws FileNotFoundException {
    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DcTerm.license.qualifiedName(), "CC-BY NC");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();
    Vocab vocab = NBNLicense.getInstance(null);

    // When
    BiConsumer<ExtendedRecord, BasicRecord> consumer = NBNBasicInterpreter.interpretLicense(vocab);
    consumer.accept(er, br);

    // Should
    assertEquals("CC-BY-NC", br.getLicense());
    assertTrue(br.getIssues().getIssueList().isEmpty());
  }

  @Test
  public void interpretLicenseNonMatchingTest() throws FileNotFoundException {
    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DcTerm.license.qualifiedName(), "not a license");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();
    Vocab vocab = NBNLicense.getInstance(null);

    // When
    BiConsumer<ExtendedRecord, BasicRecord> consumer = NBNBasicInterpreter.interpretLicense(vocab);
    consumer.accept(er, br);

    // Should
    assertEquals("UNSUPPORTED", br.getLicense());
    assertTrue(br.getIssues().getIssueList().isEmpty());
  }

  @Test
  public void interpretNoLicenseTest() throws FileNotFoundException {
    // State
    Map<String, String> coreMap = new HashMap<>();
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();
    Vocab vocab = NBNLicense.getInstance(null);

    // When
    BiConsumer<ExtendedRecord, BasicRecord> consumer = NBNBasicInterpreter.interpretLicense(vocab);
    consumer.accept(er, br);

    // Should
    assertEquals("UNSPECIFIED", br.getLicense());
    assertTrue(br.getIssues().getIssueList().isEmpty());
  }

  @Test
  public void
      givenOccurrenceBasisOfRecord_whenInterpretBasisOfRecord_shouldChangeToHumanObservation() {
    // State
    Map<String, String> coreMap = new HashMap<>();
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    BasicRecord br =
        BasicRecord.newBuilder()
            .setId(ID)
            .setBasisOfRecord(BasisOfRecord.OCCURRENCE.name())
            .build();

    // When
    NBNBasicInterpreter.interpretBasisOfRecord(er, br);

    // Should
    assertEquals(BasisOfRecord.HUMAN_OBSERVATION.name(), br.getBasisOfRecord());
  }

  @Test
  public void givenLivingSpecimenBasisOfRecord_whenInterpretBasisOfRecord_shouldNotChange() {
    // State
    Map<String, String> coreMap = new HashMap<>();
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    BasicRecord br =
        BasicRecord.newBuilder()
            .setId(ID)
            .setBasisOfRecord(BasisOfRecord.LIVING_SPECIMEN.name())
            .build();

    // When
    NBNBasicInterpreter.interpretBasisOfRecord(er, br);

    // Should
    assertEquals(BasisOfRecord.LIVING_SPECIMEN.name(), br.getBasisOfRecord());
  }

  @Test
  public void givenNullBasisOfRecord_whenInterpretBasisOfRecord_shouldNotThrowException() {
    // State
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    try {
      NBNBasicInterpreter.interpretBasisOfRecord(er, br);
    } catch (Exception e) {
      // should
      fail("Method threw an exception: " + e.getMessage());
    }
  }

  @Test
  public void
      givenMuseumSpecimensBasisOfRecord_whenInterpretBasisOfRecord_shouldInterpretAsPreservedSpecimen() {
    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DwcTerm.basisOfRecord.qualifiedName(), "museum specimens");

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretBasisOfRecord(er, br);
    NBNBasicInterpreter.interpretBasisOfRecord(er, br);

    // Should
    assertEquals(BasisOfRecord.PRESERVED_SPECIMEN.name(), br.getBasisOfRecord());
  }

  @Test
  public void
      givenMuseumSpecimensWithoutSpacesBasisOfRecord_whenInterpretBasisOfRecord_shouldInterpretAsPreservedSpecimen() {
    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DwcTerm.basisOfRecord.qualifiedName(), "museumspecimens");

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretBasisOfRecord(er, br);
    NBNBasicInterpreter.interpretBasisOfRecord(er, br);

    // Should
    assertEquals(BasisOfRecord.PRESERVED_SPECIMEN.name(), br.getBasisOfRecord());
  }

  @Test
  public void givenImageBasisOfRecord_whenInterpretBasisOfRecord_shouldInterpretAsObservation() {
    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DwcTerm.basisOfRecord.qualifiedName(), "image");

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    // When
    BasicInterpreter.interpretBasisOfRecord(er, br);
    NBNBasicInterpreter.interpretBasisOfRecord(er, br);

    // Should
    assertEquals(BasisOfRecord.OBSERVATION.name(), br.getBasisOfRecord());
  }

  @Test
  public void givenNullIdentificationVerificationStatus_whenInterpret_shouldAddMissingIssue()
      throws FileNotFoundException {
    // State
    Map<String, String> coreMap = new HashMap<>();
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();
    Vocab vocab = IdentificationVerificationStatus.getInstance(null);

    // When
    BiConsumer<ExtendedRecord, BasicRecord> consumer =
        NBNBasicInterpreter.interpretIdentificationVerificationStatus(vocab);
    consumer.accept(er, br);

    // Should
    assertEquals("Unconfirmed", br.getIdentificationVerificationStatus());
    assertTrue(
        br.getIssues()
            .getIssueList()
            .contains(NBNOccurrenceIssue.MISSING_IDENTIFICATIONVERIFICATIONSTATUS.name()));
  }

  @Test
  public void givenEmptyIdentificationVerificationStatus_whenInterpret_shouldAddMissingIssue()
      throws FileNotFoundException {
    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DwcTerm.identificationVerificationStatus.qualifiedName(), "");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();
    Vocab vocab = IdentificationVerificationStatus.getInstance(null);

    // When
    BiConsumer<ExtendedRecord, BasicRecord> consumer =
        NBNBasicInterpreter.interpretIdentificationVerificationStatus(vocab);
    consumer.accept(er, br);

    // Should
    assertEquals("Unconfirmed", br.getIdentificationVerificationStatus());
    assertTrue(
        br.getIssues()
            .getIssueList()
            .contains(NBNOccurrenceIssue.MISSING_IDENTIFICATIONVERIFICATIONSTATUS.name()));
  }

  @Test
  public void
      givenUnmatchedIdentificationVerificationStatus_whenInterpret_shouldAddUnrecognisedIssue()
          throws FileNotFoundException {
    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DwcTerm.identificationVerificationStatus.qualifiedName(), "not valid");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();
    Vocab vocab = IdentificationVerificationStatus.getInstance(null);

    // When
    BiConsumer<ExtendedRecord, BasicRecord> consumer =
        NBNBasicInterpreter.interpretIdentificationVerificationStatus(vocab);
    consumer.accept(er, br);

    // Should
    assertTrue(
        br.getIssues()
            .getIssueList()
            .contains(NBNOccurrenceIssue.UNRECOGNISED_IDENTIFICATIONVERIFICATIONSTATUS.name()));
  }

  @Test
  public void
      givenMatchedIdentificationVerificationStatus_whenInterpret_shouldSetIdentificationVerificationStatusToIt()
          throws FileNotFoundException {
    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DwcTerm.identificationVerificationStatus.qualifiedName(), "Considered Correct");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();
    Vocab vocab = IdentificationVerificationStatus.getInstance(null);

    // When
    BiConsumer<ExtendedRecord, BasicRecord> consumer =
        NBNBasicInterpreter.interpretIdentificationVerificationStatus(vocab);
    consumer.accept(er, br);

    // Should
    assertEquals("Accepted - considered correct", br.getIdentificationVerificationStatus());
  }

  @Test
  public void givenOccurrenceStatusValid_whenNBNInterpretOccurrenceStatus_shouldAddNoIssues()
      throws FileNotFoundException {
    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DwcTerm.occurrenceStatus.qualifiedName(), OccurrenceStatus.PRESENT.name());

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    KeyValueStore<String, OccurrenceStatus> vocab =
        OccurrenceStatusKvStoreFactory.getInstanceSupplier(TestUtils.getConfig().getGbifConfig())
            .get();
    // When
    BiConsumer<ExtendedRecord, BasicRecord> consumer =
        BasicInterpreter.interpretOccurrenceStatus(vocab);
    consumer.accept(er, br);

    BiConsumer<ExtendedRecord, BasicRecord> NBNconsumer =
        NBNBasicInterpreter.interpretOccurrenceStatus(vocab);
    NBNconsumer.accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.PRESENT.name(), br.getOccurrenceStatus());
    assertTrue(br.getIssues().getIssueList().isEmpty());
  }

  @Test
  public void
      givenOccurrenceStatusMissingAndIndividualCountValid_whenNBNInterpretOccurrenceStatus_shouldAddNoIssues()
          throws FileNotFoundException {
    // State
    Map<String, String> coreMap = new HashMap<>();
    coreMap.put(DwcTerm.individualCount.qualifiedName(), "1");

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    final List<String> expectedALAIssues =
        Arrays.asList(OccurrenceIssue.OCCURRENCE_STATUS_INFERRED_FROM_INDIVIDUAL_COUNT.name());

    KeyValueStore<String, OccurrenceStatus> vocab =
        OccurrenceStatusKvStoreFactory.getInstanceSupplier(TestUtils.getConfig().getGbifConfig())
            .get();
    // When
    BiConsumer<ExtendedRecord, BasicRecord> consumer =
        BasicInterpreter.interpretOccurrenceStatus(vocab);
    consumer.accept(er, br);

    assertEquals(OccurrenceStatus.PRESENT.name(), br.getOccurrenceStatus());
    assertEquals(expectedALAIssues, br.getIssues().getIssueList());

    BiConsumer<ExtendedRecord, BasicRecord> NBNconsumer =
        NBNBasicInterpreter.interpretOccurrenceStatus(vocab);
    NBNconsumer.accept(er, br);

    // Should
    assertEquals(expectedALAIssues, br.getIssues().getIssueList());
  }

  @Test
  public void
      givenMissingOccurrenceStatusAndMissingCount_whenNBNInterpretOccurrenceStatus_shouldAddAssumedPresentIssue()
          throws FileNotFoundException {
    // State
    Map<String, String> coreMap = new HashMap<>();
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    final List<String> expectedNBNIssues =
        Arrays.asList(NBNOccurrenceIssue.OCCURRENCE_STATUS_ASSUMED_PRESENT.name());

    KeyValueStore<String, OccurrenceStatus> vocab =
        OccurrenceStatusKvStoreFactory.getInstanceSupplier(TestUtils.getConfig().getGbifConfig())
            .get();
    // When
    BiConsumer<ExtendedRecord, BasicRecord> consumer =
        BasicInterpreter.interpretOccurrenceStatus(vocab);
    consumer.accept(er, br);

    assertEquals(OccurrenceStatus.PRESENT.name(), br.getOccurrenceStatus());
    assertTrue(br.getIssues().getIssueList().isEmpty());

    BiConsumer<ExtendedRecord, BasicRecord> NBNconsumer =
        NBNBasicInterpreter.interpretOccurrenceStatus(vocab);
    NBNconsumer.accept(er, br);

    // Should
    assertEquals(expectedNBNIssues, br.getIssues().getIssueList());
  }

  @Test
  public void
      givenInvalidOccurrenceStatusAndMissingCount_whenNBNInterpretOccurrenceStatus_shouldAddAssumedPresentIssue()
          throws FileNotFoundException {
    // State
    Map<String, String> coreMap = new HashMap<>();

    coreMap.put(DwcTerm.occurrenceStatus.qualifiedName(), "someinvalidvalue");

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    final List<String> expectedALAIssues =
        Arrays.asList(OccurrenceIssue.OCCURRENCE_STATUS_UNPARSABLE.name());

    final List<String> expectedNBNIssues =
        Arrays.asList(NBNOccurrenceIssue.OCCURRENCE_STATUS_ASSUMED_PRESENT.name());

    final List<String> expectedIssues =
        Stream.concat(expectedALAIssues.stream(), expectedNBNIssues.stream())
            .collect(Collectors.toList());

    KeyValueStore<String, OccurrenceStatus> vocab =
        OccurrenceStatusKvStoreFactory.getInstanceSupplier(TestUtils.getConfig().getGbifConfig())
            .get();
    // When
    BiConsumer<ExtendedRecord, BasicRecord> consumer =
        BasicInterpreter.interpretOccurrenceStatus(vocab);
    consumer.accept(er, br);

    assertEquals(OccurrenceStatus.PRESENT.name(), br.getOccurrenceStatus());
    assertEquals(expectedALAIssues, br.getIssues().getIssueList());

    BiConsumer<ExtendedRecord, BasicRecord> NBNconsumer =
        NBNBasicInterpreter.interpretOccurrenceStatus(vocab);
    NBNconsumer.accept(er, br);

    // Should
    assertEquals(expectedIssues, br.getIssues().getIssueList());
  }

  @Test
  public void
      givenMissingOccurrenceStatusAndInvalidCount_whenNBNInterpretOccurrenceStatus_shouldAddAssumedPresentIssue()
          throws FileNotFoundException {
    // State
    Map<String, String> coreMap = new HashMap<>();

    coreMap.put(DwcTerm.individualCount.qualifiedName(), "someinvalidvalue");

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    final List<String> expectedALAIssues =
        Arrays.asList(OccurrenceIssue.INDIVIDUAL_COUNT_INVALID.name());

    final List<String> expectedNBNIssues =
        Arrays.asList(NBNOccurrenceIssue.OCCURRENCE_STATUS_ASSUMED_PRESENT.name());

    final List<String> expectedIssues =
        Stream.concat(expectedALAIssues.stream(), expectedNBNIssues.stream())
            .collect(Collectors.toList());

    KeyValueStore<String, OccurrenceStatus> vocab =
        OccurrenceStatusKvStoreFactory.getInstanceSupplier(TestUtils.getConfig().getGbifConfig())
            .get();
    // When
    BiConsumer<ExtendedRecord, BasicRecord> consumer =
        BasicInterpreter.interpretOccurrenceStatus(vocab);
    consumer.accept(er, br);

    assertEquals(OccurrenceStatus.PRESENT.name(), br.getOccurrenceStatus());
    assertEquals(expectedALAIssues, br.getIssues().getIssueList());

    BiConsumer<ExtendedRecord, BasicRecord> NBNconsumer =
        NBNBasicInterpreter.interpretOccurrenceStatus(vocab);
    NBNconsumer.accept(er, br);

    // Should
    assertEquals(expectedIssues, br.getIssues().getIssueList());
  }

  @Test
  public void
      givenInvalidOccurrenceStatusAndInvalidCount_whenNBNInterpretOccurrenceStatus_shouldAddAssumedPresentIssue()
          throws FileNotFoundException {
    // State
    Map<String, String> coreMap = new HashMap<>();

    coreMap.put(DwcTerm.individualCount.qualifiedName(), "someinvalidvalue");
    coreMap.put(DwcTerm.occurrenceStatus.qualifiedName(), "someinvalidvalue");

    ExtendedRecord er = ExtendedRecord.newBuilder().setId(ID).setCoreTerms(coreMap).build();
    BasicRecord br = BasicRecord.newBuilder().setId(ID).build();

    final List<String> expectedALAIssues =
        Arrays.asList(
            OccurrenceIssue.OCCURRENCE_STATUS_UNPARSABLE.name(),
            OccurrenceIssue.INDIVIDUAL_COUNT_INVALID.name());

    final List<String> expectedNBNIssues =
        Arrays.asList(NBNOccurrenceIssue.OCCURRENCE_STATUS_ASSUMED_PRESENT.name());

    final List<String> expectedIssues =
        Stream.concat(expectedALAIssues.stream(), expectedNBNIssues.stream())
            .collect(Collectors.toList());

    KeyValueStore<String, OccurrenceStatus> vocab =
        OccurrenceStatusKvStoreFactory.getInstanceSupplier(TestUtils.getConfig().getGbifConfig())
            .get();
    // When
    BiConsumer<ExtendedRecord, BasicRecord> consumer =
        BasicInterpreter.interpretOccurrenceStatus(vocab);
    consumer.accept(er, br);

    BiConsumer<ExtendedRecord, BasicRecord> NBNconsumer =
        NBNBasicInterpreter.interpretOccurrenceStatus(vocab);
    NBNconsumer.accept(er, br);

    // Should
    assertEquals(OccurrenceStatus.PRESENT.name(), br.getOccurrenceStatus());
    assertEquals(expectedIssues, br.getIssues().getIssueList());
  }
}
