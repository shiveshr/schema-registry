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
import io.pravega.schemaregistry.contract.generated.rest.model.VersionInfo;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import javax.validation.constraints.*;

/**
 * GetSchemaForObjectTypeByVersionRequest
 */

public class GetSchemaForObjectTypeByVersionRequest   {
  @JsonProperty("versionInfo")
  private VersionInfo versionInfo = null;

  public GetSchemaForObjectTypeByVersionRequest versionInfo(VersionInfo versionInfo) {
    this.versionInfo = versionInfo;
    return this;
  }

  /**
   * Get versionInfo
   * @return versionInfo
   **/
  @JsonProperty("versionInfo")
  @ApiModelProperty(value = "")
  public VersionInfo getVersionInfo() {
    return versionInfo;
  }

  public void setVersionInfo(VersionInfo versionInfo) {
    this.versionInfo = versionInfo;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GetSchemaForObjectTypeByVersionRequest getSchemaForObjectTypeByVersionRequest = (GetSchemaForObjectTypeByVersionRequest) o;
    return Objects.equals(this.versionInfo, getSchemaForObjectTypeByVersionRequest.versionInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(versionInfo);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class GetSchemaForObjectTypeByVersionRequest {\n");
    
    sb.append("    versionInfo: ").append(toIndentedString(versionInfo)).append("\n");
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

