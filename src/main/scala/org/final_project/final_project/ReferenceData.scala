package org.final_project.final_project

import org.apache.spark.sql.types.StructType

// Define a common trait for reference table schemas
trait ReferenceTableSchema {
  def schema: StructType
}

// Implement the trait for each reference table
object CarrierMapping extends ReferenceTableSchema {
  override def schema: StructType = new StructType()
    .add("Code", "String")
    .add("Description", "String")
}

object OriginMapping extends ReferenceTableSchema {
  override def schema: StructType = new StructType()
    .add("Code", "String")
    .add("Description", "String")
}
