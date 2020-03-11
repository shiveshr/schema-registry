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
import io.pravega.schemaregistry.contract.generated.rest.model.SchemaValidationRule;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import javax.validation.constraints.*;

/**
 * AddSchemaValidationRuleRequest
 */

public class AddSchemaValidationRuleRequest   {
  @JsonProperty("validationRules")
  private SchemaValidationRule validationRules = null;

  public AddSchemaValidationRuleRequest validationRules(SchemaValidationRule validationRules) {
    this.validationRules = validationRules;
    return this;
  }

  /**
   * Get validationRules
   * @return validationRules
   **/
  @JsonProperty("validationRules")
  @ApiModelProperty(value = "")
  public SchemaValidationRule getValidationRules() {
    return validationRules;
  }

  public void setValidationRules(SchemaValidationRule validationRules) {
    this.validationRules = validationRules;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AddSchemaValidationRuleRequest addSchemaValidationRuleRequest = (AddSchemaValidationRuleRequest) o;
    return Objects.equals(this.validationRules, addSchemaValidationRuleRequest.validationRules);
  }

  @Override
  public int hashCode() {
    return Objects.hash(validationRules);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class AddSchemaValidationRuleRequest {\n");
    
    sb.append("    validationRules: ").append(toIndentedString(validationRules)).append("\n");
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

