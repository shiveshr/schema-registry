/*
 * Pravega Schema Registry APIs
 * REST APIs for Pravega Schema Registry.
 *
 * OpenAPI spec version: 0.0.1
 * 
 *
 * NOTE: This class is auto generated by the swagger code generator program.
 * https://github.com/swagger-api/swagger-codegen.git
 * Do not edit the class manually.
 */


package io.pravega.schemaregistry.contract.generated.rest.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaReferenceRecord;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.ArrayList;
import java.util.List;
import javax.validation.constraints.*;

/**
 * SchemaReferences
 */

public class SchemaReferences   {
  @JsonProperty("references")
  private List<SchemaReferenceRecord> references = null;

  public SchemaReferences references(List<SchemaReferenceRecord> references) {
    this.references = references;
    return this;
  }

  public SchemaReferences addReferencesItem(SchemaReferenceRecord referencesItem) {
    if (this.references == null) {
      this.references = new ArrayList<SchemaReferenceRecord>();
    }
    this.references.add(referencesItem);
    return this;
  }

  /**
   * Schema versions in groups where this schema is used.
   * @return references
   **/
  @JsonProperty("references")
  @ApiModelProperty(value = "Schema versions in groups where this schema is used.")
  public List<SchemaReferenceRecord> getReferences() {
    return references;
  }

  public void setReferences(List<SchemaReferenceRecord> references) {
    this.references = references;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SchemaReferences schemaReferences = (SchemaReferences) o;
    return Objects.equals(this.references, schemaReferences.references);
  }

  @Override
  public int hashCode() {
    return Objects.hash(references);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class SchemaReferences {\n");
    
    sb.append("    references: ").append(toIndentedString(references)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(java.lang.Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}

