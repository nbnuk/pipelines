package au.org.ala.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.concurrent.Callable;
import org.jetbrains.annotations.NotNull;
import org.junit.BeforeClass;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

public class CombinedYamlConfigurationTest {
  private static CombinedYamlConfiguration testConf;

  @BeforeClass
  public static void loadConf() throws FileNotFoundException {
    testConf =
        new CombinedYamlConfiguration(
            new String[] {
              "--someArg=1",
              "--runner=other",
              "--datasetId=dr893",
              "--fsPath=/data",
              "--config=target/test-classes/pipelines.yaml,src/test/resources/pipelines-local.yaml"
            });
  }

  public static Throwable exceptionOf(Callable<?> callable) {
    try {
      callable.call();
      return null;
    } catch (Throwable t) {
      return t;
    }
  }

  @Test
  public void getUnknownValueReturnsEmptyList() throws FileNotFoundException {
    assertThat(
        new CombinedYamlConfiguration(new String[] {"--config=target/test-classes/pipelines.yaml"})
            .subSet("general2")
            .size(),
        equalTo(0));
  }

  @Test
  public void weCanJoinSeveralConfigsAndConvertToArgs() {
    String[] args = testConf.toArgs("general", "interpret");
    // it should be --args=value arrays
    assertThat(args.length, greaterThan(0));
    LinkedHashMap<String, Object> argsInMap = argsToMap(args);
    assertThat(argsInMap.get("interpretationTypes"), equalTo("ALL"));
    assertThat(argsInMap.get("runner"), equalTo("other")); // as main args has preference
    assertThat(argsInMap.get("attempt"), equalTo("1"));
    assertThat(argsInMap.get("targetPath"), equalTo("/some-other-moint-point/pipelines-data"));
    assertThat(argsInMap.get("missingVar"), equalTo(null));
    assertThat(argsInMap.get("missing.dot.var"), equalTo(null));
  }

  @NotNull
  private LinkedHashMap<String, Object> argsToMap(String[] args) {
    LinkedHashMap<String, Object> argsInMap = new LinkedHashMap<>();
    for (String arg : args) {
      assertThat(arg.substring(0, 2), equalTo("--"));
      String[] splitted = arg.substring(2).split("=", 2);
      assertThat(splitted.length, equalTo(2));
      argsInMap.put(splitted[0], splitted[1]);
    }
    return argsInMap;
  }

  @Test
  public void weCanJoinSeveralConfigsAndConvertToArgsWithParams() {
    String[] args = testConf.toArgs("general", "interpret");
    LinkedHashMap<String, Object> argsInMap = argsToMap(args);
    assertThat(argsInMap.get("name"), equalTo("interpret dr893"));
    assertThat(argsInMap.get("appName"), equalTo("Interpretation for dr893"));
    assertThat(argsInMap.get("inputPath"), equalTo("/data/pipelines-data/dr893/1/verbatim.avro"));
    assertThat(argsInMap.get("fsPath"), is(nullValue()));
  }

  @Test
  public void weSubstituteYamlValuesWithArgs() {
    String[] args = testConf.toArgs("general", "export-latlng");
    LinkedHashMap<String, Object> argsInMap = argsToMap(args);
    assertThat(argsInMap.get("appName"), equalTo("Lat Long export for dr893"));
    assertThat(argsInMap.get("inputPath"), equalTo("/data/pipelines-data"));
  }

  @Test
  public void weCanJoinSeveralConfigs() {
    LinkedHashMap<String, Object> embedConf = testConf.subSet("general", "interpret");
    assertThat(embedConf.get("interpretationTypes"), equalTo("ALL"));
    assertThat(embedConf.get("runner"), equalTo("other")); // as main args has preference
    assertThat(embedConf.get("attempt"), equalTo(1));
    assertThat(embedConf.get("missingVar"), equalTo(null));
    assertThat(embedConf.get("missing.dot.var"), equalTo(null));
  }

  @Test
  public void rootVars() {
    assertThat(testConf.subSet("root-test").getClass(), equalTo(LinkedHashMap.class));
    assertThat(testConf.subSet().get("root-test"), equalTo(1));
    assertThat(testConf.get("root-test"), equalTo(1));
    // from local.yaml:
    assertThat(testConf.get("root-test2"), equalTo(2));
  }

  @Test
  public void dotVars() {
    assertThat(testConf.get("index").getClass(), equalTo(LinkedHashMap.class));
    assertThat(testConf.get("index.includeSampling"), equalTo(true));
    assertThat(testConf.get("index.solrCollection"), equalTo("biocache"));
  }

  @Test
  public void testEmptyVarNotNull() {
    assertThat(testConf.get("general.hdfsSiteConfig"), equalTo(""));
  }

  @Test
  public void expectExceptionWhenMissingConf() throws FileNotFoundException {
    assertThat(
        exceptionOf(
            () ->
                new CombinedYamlConfiguration(
                    new String[] {"--config=src/test/resources/missing-la-pipelines.yaml"})),
        instanceOf(FileNotFoundException.class));
  }

  @Test
  public void expectExceptionWhenMissingConfigArgument() throws FileNotFoundException {
    assertThat(
        exceptionOf(() -> new CombinedYamlConfiguration(new String[] {})),
        instanceOf(RuntimeException.class));
  }

  @Test
  public void testYamlDump() throws IOException {
    String yamlPath = testConf.toYamlFile();
    String yamlStr = new String(Files.readAllBytes(Paths.get(yamlPath)));

    assertThat(yamlStr.length(), greaterThan(0));
    assertThat(yamlStr.contains("{fsPath}"), equalTo(false));
    assertThat(yamlStr.contains("{datasetId}"), equalTo(false));

    Yaml yaml = new Yaml();
    LinkedHashMap<String, Object> map = yaml.load(yamlStr);
    assertThat(
        (String) ((LinkedHashMap<String, Object>) map.get("alaNameMatch")).get("wsUrl"),
        equalTo("http://localhost:9179"));
    assertThat(
        (String) (map.get("unicode-test")),
        equalTo("Лорем ипсум долор сит амет, дуо еа прима семпер"));
  }
}
