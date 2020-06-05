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
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import javax.validation.constraints.*;

/**
 * Encoding id that uniquely identifies a schema version and codec type pair.
 */
@ApiModel(description = "Encoding id that uniquely identifies a schema version and codec type pair.")

public class EncodingId   {
  @JsonProperty("encodingId")
  private Integer encodingId = null;

  public EncodingId encodingId(Integer encodingId) {
    this.encodingId = encodingId;
    return this;
  }

  /**
   * encoding id generated by service.
   * @return encodingId
   **/
  @JsonProperty("encodingId")
  @ApiModelProperty(required = true, value = "encoding id generated by service.")
  @NotNull
  public Integer getEncodingId() {
    return encodingId;
  }

  public void setEncodingId(Integer encodingId) {
    this.encodingId = encodingId;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EncodingId encodingId = (EncodingId) o;
    return Objects.equals(this.encodingId, encodingId.encodingId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(encodingId);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class EncodingId {\n");
    
    sb.append("    encodingId: ").append(toIndentedString(encodingId)).append("\n");
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

