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


package io.pravega.schemaregistry.server.rest.generated.model;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import io.pravega.schemaregistry.server.rest.generated.model.Compatibility;
import io.pravega.schemaregistry.server.rest.generated.model.SchemaType;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import javax.validation.constraints.*;

/**
 * GroupProperty
 */

public class GroupProperty   {
  @JsonProperty("groupName")
  private String groupName = null;

  @JsonProperty("compatibilityPolicy")
  private Compatibility compatibilityPolicy = null;

  @JsonProperty("schemaType")
  private SchemaType schemaType = null;

  @JsonProperty("subgroupBySchemaName")
  private Boolean subgroupBySchemaName = null;

  @JsonProperty("enableEncoding")
  private Boolean enableEncoding = null;

  public GroupProperty groupName(String groupName) {
    this.groupName = groupName;
    return this;
  }

  /**
   * Get groupName
   * @return groupName
   **/
  @JsonProperty("groupName")
  @ApiModelProperty(value = "")
  public String getGroupName() {
    return groupName;
  }

  public void setGroupName(String groupName) {
    this.groupName = groupName;
  }

  public GroupProperty compatibilityPolicy(Compatibility compatibilityPolicy) {
    this.compatibilityPolicy = compatibilityPolicy;
    return this;
  }

  /**
   * Get compatibilityPolicy
   * @return compatibilityPolicy
   **/
  @JsonProperty("compatibilityPolicy")
  @ApiModelProperty(value = "")
  public Compatibility getCompatibilityPolicy() {
    return compatibilityPolicy;
  }

  public void setCompatibilityPolicy(Compatibility compatibilityPolicy) {
    this.compatibilityPolicy = compatibilityPolicy;
  }

  public GroupProperty schemaType(SchemaType schemaType) {
    this.schemaType = schemaType;
    return this;
  }

  /**
   * Get schemaType
   * @return schemaType
   **/
  @JsonProperty("schemaType")
  @ApiModelProperty(value = "")
  public SchemaType getSchemaType() {
    return schemaType;
  }

  public void setSchemaType(SchemaType schemaType) {
    this.schemaType = schemaType;
  }

  public GroupProperty subgroupBySchemaName(Boolean subgroupBySchemaName) {
    this.subgroupBySchemaName = subgroupBySchemaName;
    return this;
  }

  /**
   * Get subgroupBySchemaName
   * @return subgroupBySchemaName
   **/
  @JsonProperty("subgroupBySchemaName")
  @ApiModelProperty(value = "")
  public Boolean getSubgroupBySchemaName() {
    return subgroupBySchemaName;
  }

  public void setSubgroupBySchemaName(Boolean subgroupBySchemaName) {
    this.subgroupBySchemaName = subgroupBySchemaName;
  }

  public GroupProperty enableEncoding(Boolean enableEncoding) {
    this.enableEncoding = enableEncoding;
    return this;
  }

  /**
   * Get enableEncoding
   * @return enableEncoding
   **/
  @JsonProperty("enableEncoding")
  @ApiModelProperty(value = "")
  public Boolean getEnableEncoding() {
    return enableEncoding;
  }

  public void setEnableEncoding(Boolean enableEncoding) {
    this.enableEncoding = enableEncoding;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GroupProperty groupProperty = (GroupProperty) o;
    return Objects.equals(this.groupName, groupProperty.groupName) &&
        Objects.equals(this.compatibilityPolicy, groupProperty.compatibilityPolicy) &&
        Objects.equals(this.schemaType, groupProperty.schemaType) &&
        Objects.equals(this.subgroupBySchemaName, groupProperty.subgroupBySchemaName) &&
        Objects.equals(this.enableEncoding, groupProperty.enableEncoding);
  }

  @Override
  public int hashCode() {
    return Objects.hash(groupName, compatibilityPolicy, schemaType, subgroupBySchemaName, enableEncoding);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class GroupProperty {\n");
    
    sb.append("    groupName: ").append(toIndentedString(groupName)).append("\n");
    sb.append("    compatibilityPolicy: ").append(toIndentedString(compatibilityPolicy)).append("\n");
    sb.append("    schemaType: ").append(toIndentedString(schemaType)).append("\n");
    sb.append("    subgroupBySchemaName: ").append(toIndentedString(subgroupBySchemaName)).append("\n");
    sb.append("    enableEncoding: ").append(toIndentedString(enableEncoding)).append("\n");
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

