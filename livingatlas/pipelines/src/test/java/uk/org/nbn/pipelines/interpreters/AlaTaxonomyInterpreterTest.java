package uk.org.nbn.pipelines.interpreters;

import static org.junit.Assert.*;

import au.org.ala.kvs.client.ALACollectoryMetadata;
import au.org.ala.names.ws.api.NameSearch;
import au.org.ala.names.ws.api.NameUsageMatch;
import au.org.ala.pipelines.interpreters.ALATaxonomyInterpreter;
import java.util.*;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.io.avro.ALATaxonRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AlaTaxonomyInterpreterTest {
  private static final String DATARESOURCE_UID = "drTest";

  private ALACollectoryMetadata dataResource;
  private Map<NameSearch, NameUsageMatch> nameMap;
  private KeyValueStore<NameSearch, NameUsageMatch> lookup;
  private Map<String, Boolean> kingdomMap;
  private KeyValueStore<String, Boolean> kingdomLookup;

  @Before
  public void setUp() {
    Map<String, String> defaults = new HashMap<>();
    defaults.put("kingdom", "Plantae");
    List<Map<String, String>> hints = new ArrayList<>();
    hints.add(Collections.singletonMap("phylum", "Charophyta"));
    hints.add(Collections.singletonMap("phylum", "Bryophyta"));

    this.dataResource =
        ALACollectoryMetadata.builder()
            .name("Test data resource")
            .uid(DATARESOURCE_UID)
            .defaultDarwinCoreValues(defaults)
            .taxonomyCoverageHints(hints)
            .build();
    Map<String, List<String>> hintMap = this.dataResource.getHintMap();
    this.nameMap = new HashMap<>();
    // Simple lookup
    NameSearch search =
        NameSearch.builder()
            .kingdom("Plantae")
            .scientificName("Acacia dealbata")
            .hints(hintMap)
            .build();
    NameUsageMatch match =
        NameUsageMatch.builder()
            .success(true)
            .establishmentMeans("establishmentMeans")
            .habitat("habitat1/habitat2")
            .nomenclaturalStatus("nomenclaturalStatus")
            .scientificNameAuthorship("scientificNameAuthorship")
            .matchType("exactMatch")
            .issues(Collections.singletonList("noIssue"))
            .build();
    this.nameMap.put(search, match);

    this.lookup =
        new KeyValueStore<NameSearch, NameUsageMatch>() {
          @Override
          public void close() {}

          @Override
          public NameUsageMatch get(NameSearch o) {
            return nameMap.getOrDefault(o, NameUsageMatch.FAIL);
          }
        };
    this.kingdomMap = new HashMap<>();
    this.kingdomMap.put("Animalia", true);
    this.kingdomMap.put("Gronk", false);
    this.kingdomLookup =
        new KeyValueStore<String, Boolean>() {
          @Override
          public void close() {}

          @Override
          public Boolean get(String o) {
            return kingdomMap.get(o);
          }
        };
  }

  @After
  public void tearDown() throws Exception {
    this.lookup.close();
  }

  @Test
  public void testNBNAddedFields() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.scientificName.qualifiedName(), "Acacia dealbata");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    ALATaxonRecord atr = ALATaxonRecord.newBuilder().setId("1").build();
    ALATaxonomyInterpreter.alaTaxonomyInterpreter(this.dataResource, this.lookup, false)
        .accept(er, atr);
    assertEquals("establishmentMeans", atr.getEstablishmentMeansTaxon());
    assertEquals("nomenclaturalStatus", atr.getNomenclaturalStatus());
    assertEquals(Arrays.asList("habitat1", "habitat2"), atr.getHabitatsTaxon());
    assertEquals("scientificNameAuthorship", atr.getScientificNameAuthorship());
    assertTrue(atr.getIssues().getIssueList().isEmpty());
  }
}
