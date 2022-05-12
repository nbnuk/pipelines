package org.gbif.pipelines.ingest.java.transforms;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;
import static org.gbif.pipelines.core.utils.FsUtils.createParentDirectories;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.io.SyncDataFileWriter;
import org.gbif.pipelines.core.io.SyncDataFileWriterBuilder;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.io.avro.Record;
import org.gbif.pipelines.transforms.Transform;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class InterpretedAvroWriter {

  /** Create an AVRO file writer */
  @SneakyThrows
  public static <T extends SpecificRecordBase & Record> SyncDataFileWriter<T> createAvroWriter(
      InterpretationPipelineOptions options,
      Transform<?, T> transform,
      String id,
      String baseName) {
    String pathString =
        PathBuilder.buildPathInterpretUsingTargetPath(options, baseName, id + AVRO_EXTENSION);
    Path path = new Path(pathString);
    FileSystem fs =
        createParentDirectories(
            HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig()), path);
    return SyncDataFileWriterBuilder.builder()
        .schema(transform.getAvroSchema())
        .codec(options.getAvroCompressionType())
        .outputStream(fs.create(path))
        .syncInterval(options.getAvroSyncInterval())
        .build()
        .createSyncDataFileWriter();
  }

  public static <T extends SpecificRecordBase & Record> SyncDataFileWriter<T> createAvroWriter(
      InterpretationPipelineOptions options, Transform<?, T> transform, String id) {
    return createAvroWriter(options, transform, id, transform.getBaseName());
  }
}
