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
import com.fasterxml.jackson.annotation.JsonValue;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import javax.validation.constraints.*;

/**
 * CompressionType
 */

public class CompressionType   {
  /**
   * Gets or Sets compressionType
   */
  public enum CompressionTypeEnum {
    NONE("None"),
    
    SNAPPY("Snappy"),
    
    GZIP("Gzip"),
    
    CUSTOM("Custom");

    private String value;

    CompressionTypeEnum(String value) {
      this.value = value;
    }

    @Override
    @JsonValue
    public String toString() {
      return String.valueOf(value);
    }

    @JsonCreator
    public static CompressionTypeEnum fromValue(String text) {
      for (CompressionTypeEnum b : CompressionTypeEnum.values()) {
        if (String.valueOf(b.value).equals(text)) {
          return b;
        }
      }
      return null;
    }
  }

  @JsonProperty("compressionType")
  private CompressionTypeEnum compressionType = null;

  @JsonProperty("customTypeName")
  private String customTypeName = null;

  public CompressionType compressionType(CompressionTypeEnum compressionType) {
    this.compressionType = compressionType;
    return this;
  }

  /**
   * Get compressionType
   * @return compressionType
   **/
  @JsonProperty("compressionType")
  @ApiModelProperty(value = "")
  public CompressionTypeEnum getCompressionType() {
    return compressionType;
  }

  public void setCompressionType(CompressionTypeEnum compressionType) {
    this.compressionType = compressionType;
  }

  public CompressionType customTypeName(String customTypeName) {
    this.customTypeName = customTypeName;
    return this;
  }

  /**
   * Get customTypeName
   * @return customTypeName
   **/
  @JsonProperty("customTypeName")
  @ApiModelProperty(value = "")
  public String getCustomTypeName() {
    return customTypeName;
  }

  public void setCustomTypeName(String customTypeName) {
    this.customTypeName = customTypeName;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CompressionType compressionType = (CompressionType) o;
    return Objects.equals(this.compressionType, compressionType.compressionType) &&
        Objects.equals(this.customTypeName, compressionType.customTypeName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(compressionType, customTypeName);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class CompressionType {\n");
    
    sb.append("    compressionType: ").append(toIndentedString(compressionType)).append("\n");
    sb.append("    customTypeName: ").append(toIndentedString(customTypeName)).append("\n");
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

