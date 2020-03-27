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
import io.pravega.schemaregistry.contract.generated.rest.model.CodecType;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import javax.validation.constraints.*;

/**
 * AddCodec
 */

public class AddCodec   {
  @JsonProperty("codec")
  private CodecType codec = null;

  public AddCodec codec(CodecType codec) {
    this.codec = codec;
    return this;
  }

  /**
   * Get codec
   * @return codec
   **/
  @JsonProperty("codec")
  @ApiModelProperty(value = "")
  public CodecType getCodec() {
    return codec;
  }

  public void setCodec(CodecType codec) {
    this.codec = codec;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AddCodec addCodec = (AddCodec) o;
    return Objects.equals(this.codec, addCodec.codec);
  }

  @Override
  public int hashCode() {
    return Objects.hash(codec);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class AddCodec {\n");
    
    sb.append("    codec: ").append(toIndentedString(codec)).append("\n");
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

