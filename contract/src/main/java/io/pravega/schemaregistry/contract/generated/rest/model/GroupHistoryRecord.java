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
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaInfo;
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaValidationRules;
import io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import javax.validation.constraints.*;

/**
 * GroupHistoryRecord
 */

public class GroupHistoryRecord   {
  @JsonProperty("schemaInfo")
  private SchemaInfo schemaInfo = null;

  @JsonProperty("version")
  private VersionInfo version = null;

  @JsonProperty("validationRules")
  private SchemaValidationRules validationRules = null;

  @JsonProperty("timestamp")
  private Long timestamp = null;

  @JsonProperty("schemaString")
  private String schemaString = null;

  public GroupHistoryRecord schemaInfo(SchemaInfo schemaInfo) {
    this.schemaInfo = schemaInfo;
    return this;
  }

  /**
   * Get schemaInfo
   * @return schemaInfo
   **/
  @JsonProperty("schemaInfo")
  @ApiModelProperty(value = "")
  public SchemaInfo getSchemaInfo() {
    return schemaInfo;
  }

  public void setSchemaInfo(SchemaInfo schemaInfo) {
    this.schemaInfo = schemaInfo;
  }

  public GroupHistoryRecord version(VersionInfo version) {
    this.version = version;
    return this;
  }

  /**
   * Get version
   * @return version
   **/
  @JsonProperty("version")
  @ApiModelProperty(value = "")
  public VersionInfo getVersion() {
    return version;
  }

  public void setVersion(VersionInfo version) {
    this.version = version;
  }

  public GroupHistoryRecord validationRules(SchemaValidationRules validationRules) {
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

  public GroupHistoryRecord timestamp(Long timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  /**
   * Get timestamp
   * @return timestamp
   **/
  @JsonProperty("timestamp")
  @ApiModelProperty(value = "")
  public Long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }

  public GroupHistoryRecord schemaString(String schemaString) {
    this.schemaString = schemaString;
    return this;
  }

  /**
   * Get schemaString
   * @return schemaString
   **/
  @JsonProperty("schemaString")
  @ApiModelProperty(value = "")
  public String getSchemaString() {
    return schemaString;
  }

  public void setSchemaString(String schemaString) {
    this.schemaString = schemaString;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GroupHistoryRecord groupHistoryRecord = (GroupHistoryRecord) o;
    return Objects.equals(this.schemaInfo, groupHistoryRecord.schemaInfo) &&
        Objects.equals(this.version, groupHistoryRecord.version) &&
        Objects.equals(this.validationRules, groupHistoryRecord.validationRules) &&
        Objects.equals(this.timestamp, groupHistoryRecord.timestamp) &&
        Objects.equals(this.schemaString, groupHistoryRecord.schemaString);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schemaInfo, version, validationRules, timestamp, schemaString);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class GroupHistoryRecord {\n");
    
    sb.append("    schemaInfo: ").append(toIndentedString(schemaInfo)).append("\n");
    sb.append("    version: ").append(toIndentedString(version)).append("\n");
    sb.append("    validationRules: ").append(toIndentedString(validationRules)).append("\n");
    sb.append("    timestamp: ").append(toIndentedString(timestamp)).append("\n");
    sb.append("    schemaString: ").append(toIndentedString(schemaString)).append("\n");
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

