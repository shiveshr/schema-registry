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
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaType;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaValidationRules;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.*;

/**
 * CreateGroupRequest
 */

public class CreateGroupRequest   {
  @JsonProperty("groupName")
  private String groupName = null;

  @JsonProperty("schemaType")
  private SchemaType schemaType = null;

  @JsonProperty("validationRules")
  private SchemaValidationRules validationRules = null;

  @JsonProperty("properties")
  private Map<String, String> properties = null;

  @JsonProperty("versionBySchemaName")
  private Boolean versionBySchemaName = null;

  public CreateGroupRequest groupName(String groupName) {
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

  public CreateGroupRequest schemaType(SchemaType schemaType) {
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

  public CreateGroupRequest validationRules(SchemaValidationRules validationRules) {
    this.validationRules = validationRules;
    return this;
  }

  /**
   * Get validationRules
   * @return validationRules
   **/
  @JsonProperty("validationRules")
  @ApiModelProperty(value = "")
  public SchemaValidationRules getValidationRules() {
    return validationRules;
  }

  public void setValidationRules(SchemaValidationRules validationRules) {
    this.validationRules = validationRules;
  }

  public CreateGroupRequest properties(Map<String, String> properties) {
    this.properties = properties;
    return this;
  }

  public CreateGroupRequest putPropertiesItem(String key, String propertiesItem) {
    if (this.properties == null) {
      this.properties = new HashMap<String, String>();
    }
    this.properties.put(key, propertiesItem);
    return this;
  }

  /**
   * Get properties
   * @return properties
   **/
  @JsonProperty("properties")
  @ApiModelProperty(value = "")
  public Map<String, String> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  public CreateGroupRequest versionBySchemaName(Boolean versionBySchemaName) {
    this.versionBySchemaName = versionBySchemaName;
    return this;
  }

  /**
   * Get versionBySchemaName
   * @return versionBySchemaName
   **/
  @JsonProperty("versionBySchemaName")
  @ApiModelProperty(value = "")
  public Boolean isVersionBySchemaName() {
    return versionBySchemaName;
  }

  public void setVersionBySchemaName(Boolean versionBySchemaName) {
    this.versionBySchemaName = versionBySchemaName;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CreateGroupRequest createGroupRequest = (CreateGroupRequest) o;
    return Objects.equals(this.groupName, createGroupRequest.groupName) &&
        Objects.equals(this.schemaType, createGroupRequest.schemaType) &&
        Objects.equals(this.validationRules, createGroupRequest.validationRules) &&
        Objects.equals(this.properties, createGroupRequest.properties) &&
        Objects.equals(this.versionBySchemaName, createGroupRequest.versionBySchemaName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(groupName, schemaType, validationRules, properties, versionBySchemaName);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class CreateGroupRequest {\n");
    
    sb.append("    groupName: ").append(toIndentedString(groupName)).append("\n");
    sb.append("    schemaType: ").append(toIndentedString(schemaType)).append("\n");
    sb.append("    validationRules: ").append(toIndentedString(validationRules)).append("\n");
    sb.append("    properties: ").append(toIndentedString(properties)).append("\n");
    sb.append("    versionBySchemaName: ").append(toIndentedString(versionBySchemaName)).append("\n");
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

