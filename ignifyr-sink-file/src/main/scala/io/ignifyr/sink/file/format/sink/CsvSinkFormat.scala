package io.ignifyr.sink.file.format.sink

import io.ignifyr.sink.file.format.{FileSinkFormat, FileSinkSupport}
import io.ignifyr.engine.model.{FhirMappingResult, FileSystemSinkSettings, SinkContentTypes}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Community CSV file sink format. CSV is a flat structure, so only primitive (non-array, non-struct)
 * columns of the mapped resources are written. Optionally partitions the output by FHIR resource type;
 * when it does, the `resourceType` column is dropped per resource-type folder since it is redundant
 * given the folder name, and the per-resource-type `partitioningColumns` sub-partitioning is not
 * applicable to CSV.
 */
class CsvSinkFormat extends FileSinkFormat {

  override val contentTypes: Seq[String] = Seq(SinkContentTypes.CSV)

  override def write(
      spark: SparkSession,
      df: Dataset[FhirMappingResult],
      sinkSettings: FileSystemSinkSettings
  ): Unit = {
    import spark.implicits._
    if (sinkSettings.partitionByResourceType) {
      FileSinkSupport.writePartitionedByResourceType(
        spark,
        df,
        sinkSettings,
        singleColumnJson = false,
        flattenNonNestedColumns = true,
        writeGroup = (writer, outputPath) => writer.csv(outputPath)
      )
    } else {
      // read the mapped resource json column and load it to a new data frame
      val mappedResourceDF = spark.read.json(df.select("mappedFhirResource.mappedResource").as[String])
      // if the DataFrame contains data, write it to the specified path
      if (!mappedResourceDF.isEmpty) {
        // select the columns that are not array type or struct type since the CSV is a flat data structure
        val filteredDF = FileSinkSupport.filterNonNestedColumns(mappedResourceDF)
        FileSinkSupport.getWriter(filteredDF, sinkSettings).csv(sinkSettings.path)
      }
    }
  }
}
