package uk.org.nbn.pipelines.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;

/** Main pipeline options necessary for applying Access Controls */
public interface NBNInterpretedToAccessControlledPipelineOptions
    extends InterpretationPipelineOptions {

  @Description("defaultPublicResolutionInMeters")
  @Default.Integer(10000)
  Integer getDefaultPublicResolutionInMeters();

  void setDefaultPublicResolutionInMeters(Integer defaultPublicResolutionInMeters);
}
