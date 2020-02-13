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
import com.fasterxml.jackson.annotation.JsonValue;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import javax.validation.constraints.*;

/**
 * SchemaType
 */

public class SchemaType   {
  /**
   * Gets or Sets schemaType
   */
  public enum SchemaTypeEnum {
    NONE("None"),
    
    AVRO("Avro"),
    
    PROTOBUF("Protobuf"),
    
    JSON("Json"),
    
    CUSTOM("Custom");

    private String value;

    SchemaTypeEnum(String value) {
      this.value = value;
    }

    @Override
    @JsonValue
    public String toString() {
      return String.valueOf(value);
    }

    @JsonCreator
    public static SchemaTypeEnum fromValue(String text) {
      for (SchemaTypeEnum b : SchemaTypeEnum.values()) {
        if (String.valueOf(b.value).equals(text)) {
          return b;
        }
      }
      return null;
    }
  }

  @JsonProperty("schemaType")
  private SchemaTypeEnum schemaType = null;

  @JsonProperty("customTypeName")
  private String customTypeName = null;

  public SchemaType schemaType(SchemaTypeEnum schemaType) {
    this.schemaType = schemaType;
    return this;
  }

  /**
   * Get schemaType
   * @return schemaType
   **/
  @JsonProperty("schemaType")
  @ApiModelProperty(value = "")
  public SchemaTypeEnum getSchemaType() {
    return schemaType;
  }

  public void setSchemaType(SchemaTypeEnum schemaType) {
    this.schemaType = schemaType;
  }

  public SchemaType customTypeName(String customTypeName) {
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
    SchemaType schemaType = (SchemaType) o;
    return Objects.equals(this.schemaType, schemaType.schemaType) &&
        Objects.equals(this.customTypeName, schemaType.customTypeName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schemaType, customTypeName);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class SchemaType {\n");
    
    sb.append("    schemaType: ").append(toIndentedString(schemaType)).append("\n");
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

