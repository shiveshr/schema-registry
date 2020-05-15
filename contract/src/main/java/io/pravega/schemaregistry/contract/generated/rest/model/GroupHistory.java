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
import io.pravega.schemaregistry.contract.generated.rest.model.GroupHistoryRecord;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.ArrayList;
import java.util.List;
import javax.validation.constraints.*;

/**
 * GroupHistory
 */

public class GroupHistory   {
  @JsonProperty("history")
  private List<GroupHistoryRecord> history = null;

  public GroupHistory history(List<GroupHistoryRecord> history) {
    this.history = history;
    return this;
  }

  public GroupHistory addHistoryItem(GroupHistoryRecord historyItem) {
    if (this.history == null) {
      this.history = new ArrayList<GroupHistoryRecord>();
    }
    this.history.add(historyItem);
    return this;
  }

  /**
   * Chronological list of Group History records.
   * @return history
   **/
  @JsonProperty("history")
  @ApiModelProperty(value = "Chronological list of Group History records.")
  public List<GroupHistoryRecord> getHistory() {
    return history;
  }

  public void setHistory(List<GroupHistoryRecord> history) {
    this.history = history;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GroupHistory groupHistory = (GroupHistory) o;
    return Objects.equals(this.history, groupHistory.history);
  }

  @Override
  public int hashCode() {
    return Objects.hash(history);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class GroupHistory {\n");
    
    sb.append("    history: ").append(toIndentedString(history)).append("\n");
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

