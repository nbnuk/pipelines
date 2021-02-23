package org.gbif.pipelines.maven;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.gbif.dwc.digester.ThesaurusHandlingRule;
import org.gbif.dwc.extensions.Extension;
import org.gbif.dwc.extensions.ExtensionFactory;
import org.gbif.dwc.extensions.VocabulariesManager;
import org.gbif.dwc.extensions.Vocabulary;
import org.gbif.dwc.xml.SAXUtils;

@Mojo(name = "avroschemageneration", defaultPhase = LifecyclePhase.GENERATE_SOURCES)
public class XmlToAvscGeneratorMojo extends AbstractMojo {

  @Parameter(property = "avroschemageneration.extensions")
  private List<String> extensions;

  @Parameter(property = "avroschemageneration.pathToWrite")
  private String pathToWrite;

  @Parameter(property = "avroschemageneration.namespace")
  private String namespace;

  @Override
  public void execute() throws MojoExecutionException {

    try {
      Files.createDirectories(Paths.get(pathToWrite));

      for (String extension : extensions) {

        String[] ext = extension.split(",");
        URL url = new URL(ext[1]);
        convertAndWrite(ext[0], url);
      }
    } catch (Exception ex) {
      throw new MojoExecutionException(ex.getMessage());
    }
  }

  public void setExtensions(List<String> extensions) {
    this.extensions = extensions;
  }

  public void setPathToWrite(String pathToWrite) {
    this.pathToWrite = pathToWrite;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  private void convertAndWrite(String name, URL url) throws Exception {
    // Read extension
    ThesaurusHandlingRule thr = new ThesaurusHandlingRule(new EmptyVocabulariesManager());
    ExtensionFactory factory = new ExtensionFactory(thr, SAXUtils.getNsAwareSaxParserFactory());
    Extension ext = factory.build(url.openStream(), url, false);

    // Convert into an avro schema
    List<Schema.Field> fields = new ArrayList<>(ext.getProperties().size() + 1);
    // Add gbifID
    fields.add(createSchemaField("gbifid", Type.LONG, "GBIF internal identifier", false));
    // Add RAW fields
    ext.getProperties().stream()
        .map(p -> createSchemaField("v_" + nomalizeFieldName(p.getName()), p.getQualname()))
        .forEach(fields::add);
    // Add fields
    ext.getProperties().stream()
        .map(p -> createSchemaField(nomalizeFieldName(p.getName()), p.getQualname()))
        .forEach(fields::add);

    String schema =
        Schema.createRecord(name, "Avro Schema of Hive Table for " + name, namespace, false, fields)
            .toString(true);

    // Add comment
    String comment = "/** This class was genereted by xml-to-avsc-maven-plugin */\n";
    schema = comment + schema;

    // Save into a file
    Path path = Paths.get(pathToWrite, normalizeFileName(name));
    Files.deleteIfExists(path);
    getLog().info("Create avro schema for " + ext.getName() + " extension - " + path.toString());
    Files.write(path, schema.getBytes(UTF_8));
  }

  private Schema.Field createSchemaField(String name, String doc) {
    return createSchemaField(name, Type.STRING, doc, true);
  }

  private String nomalizeFieldName(String name) {
    return name.toLowerCase().trim().replace('-', '_');
  }

  private String normalizeFileName(String name) {
    String result =
        Arrays.stream(name.split("(?=[A-Z])"))
            .map(String::toLowerCase)
            .collect(Collectors.joining("-"));
    return result + ".avsc";
  }

  private Schema.Field createSchemaField(
      String name, Schema.Type type, String doc, boolean isNull) {

    Schema schema;
    if (isNull) {
      List<Schema> optionalString = new ArrayList<>(2);
      optionalString.add(Schema.create(Schema.Type.NULL));
      optionalString.add(Schema.create(type));
      schema = Schema.createUnion(optionalString);
    } else {
      schema = Schema.create(type);
    }
    return new Schema.Field(name, schema, doc, JsonProperties.NULL_VALUE);
  }

  private static class EmptyVocabulariesManager implements VocabulariesManager {

    @Override
    public Vocabulary get(String uri) {
      return null;
    }

    @Override
    public Vocabulary get(URL url) {
      return null;
    }

    @Override
    public Map<String, String> getI18nVocab(String uri, String lang) {
      return null;
    }

    @Override
    public List<Vocabulary> list() {
      return null;
    }
  }
}
