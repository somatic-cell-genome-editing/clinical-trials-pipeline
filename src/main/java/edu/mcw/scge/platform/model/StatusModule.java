
package edu.mcw.scge.platform.model;

import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.processing.Generated;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "primaryCompletionDateStruct",
    "statusVerifiedDate",
    "whyStopped",
    "studyFirstSubmitDate",
    "lastUpdateSubmitDate",
    "lastUpdatePostDateStruct",
    "studyFirstPostDateStruct",
    "overallStatus",
    "expandedAccessInfo",
    "startDateStruct",
    "completionDateStruct",
    "studyFirstSubmitQcDate"
})
@Generated("jsonschema2pojo")
public class StatusModule {

    @JsonProperty("primaryCompletionDateStruct")
    private PrimaryCompletionDateStruct primaryCompletionDateStruct;
    @JsonProperty("statusVerifiedDate")
    private String statusVerifiedDate;
    @JsonProperty("whyStopped")
    private String whyStopped;
    @JsonProperty("studyFirstSubmitDate")
    private String studyFirstSubmitDate;
    @JsonProperty("lastUpdateSubmitDate")
    private String lastUpdateSubmitDate;
    @JsonProperty("lastUpdatePostDateStruct")
    private LastUpdatePostDateStruct lastUpdatePostDateStruct;
    @JsonProperty("studyFirstPostDateStruct")
    private StudyFirstPostDateStruct studyFirstPostDateStruct;
    @JsonProperty("overallStatus")
    private String overallStatus;
    @JsonProperty("expandedAccessInfo")
    private ExpandedAccessInfo expandedAccessInfo;
    @JsonProperty("startDateStruct")
    private StartDateStruct startDateStruct;
    @JsonProperty("completionDateStruct")
    private CompletionDateStruct completionDateStruct;
    @JsonProperty("studyFirstSubmitQcDate")
    private String studyFirstSubmitQcDate;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new LinkedHashMap<String, Object>();

    @JsonProperty("primaryCompletionDateStruct")
    public PrimaryCompletionDateStruct getPrimaryCompletionDateStruct() {
        return primaryCompletionDateStruct;
    }

    @JsonProperty("primaryCompletionDateStruct")
    public void setPrimaryCompletionDateStruct(PrimaryCompletionDateStruct primaryCompletionDateStruct) {
        this.primaryCompletionDateStruct = primaryCompletionDateStruct;
    }

    public StatusModule withPrimaryCompletionDateStruct(PrimaryCompletionDateStruct primaryCompletionDateStruct) {
        this.primaryCompletionDateStruct = primaryCompletionDateStruct;
        return this;
    }

    @JsonProperty("statusVerifiedDate")
    public String getStatusVerifiedDate() {
        return statusVerifiedDate;
    }

    @JsonProperty("statusVerifiedDate")
    public void setStatusVerifiedDate(String statusVerifiedDate) {
        this.statusVerifiedDate = statusVerifiedDate;
    }

    public StatusModule withStatusVerifiedDate(String statusVerifiedDate) {
        this.statusVerifiedDate = statusVerifiedDate;
        return this;
    }

    @JsonProperty("whyStopped")
    public String getWhyStopped() {
        return whyStopped;
    }

    @JsonProperty("whyStopped")
    public void setWhyStopped(String whyStopped) {
        this.whyStopped = whyStopped;
    }

    public StatusModule withWhyStopped(String whyStopped) {
        this.whyStopped = whyStopped;
        return this;
    }

    @JsonProperty("studyFirstSubmitDate")
    public String getStudyFirstSubmitDate() {
        return studyFirstSubmitDate;
    }

    @JsonProperty("studyFirstSubmitDate")
    public void setStudyFirstSubmitDate(String studyFirstSubmitDate) {
        this.studyFirstSubmitDate = studyFirstSubmitDate;
    }

    public StatusModule withStudyFirstSubmitDate(String studyFirstSubmitDate) {
        this.studyFirstSubmitDate = studyFirstSubmitDate;
        return this;
    }

    @JsonProperty("lastUpdateSubmitDate")
    public String getLastUpdateSubmitDate() {
        return lastUpdateSubmitDate;
    }

    @JsonProperty("lastUpdateSubmitDate")
    public void setLastUpdateSubmitDate(String lastUpdateSubmitDate) {
        this.lastUpdateSubmitDate = lastUpdateSubmitDate;
    }

    public StatusModule withLastUpdateSubmitDate(String lastUpdateSubmitDate) {
        this.lastUpdateSubmitDate = lastUpdateSubmitDate;
        return this;
    }

    @JsonProperty("lastUpdatePostDateStruct")
    public LastUpdatePostDateStruct getLastUpdatePostDateStruct() {
        return lastUpdatePostDateStruct;
    }

    @JsonProperty("lastUpdatePostDateStruct")
    public void setLastUpdatePostDateStruct(LastUpdatePostDateStruct lastUpdatePostDateStruct) {
        this.lastUpdatePostDateStruct = lastUpdatePostDateStruct;
    }

    public StatusModule withLastUpdatePostDateStruct(LastUpdatePostDateStruct lastUpdatePostDateStruct) {
        this.lastUpdatePostDateStruct = lastUpdatePostDateStruct;
        return this;
    }

    @JsonProperty("studyFirstPostDateStruct")
    public StudyFirstPostDateStruct getStudyFirstPostDateStruct() {
        return studyFirstPostDateStruct;
    }

    @JsonProperty("studyFirstPostDateStruct")
    public void setStudyFirstPostDateStruct(StudyFirstPostDateStruct studyFirstPostDateStruct) {
        this.studyFirstPostDateStruct = studyFirstPostDateStruct;
    }

    public StatusModule withStudyFirstPostDateStruct(StudyFirstPostDateStruct studyFirstPostDateStruct) {
        this.studyFirstPostDateStruct = studyFirstPostDateStruct;
        return this;
    }

    @JsonProperty("overallStatus")
    public String getOverallStatus() {
        return overallStatus;
    }

    @JsonProperty("overallStatus")
    public void setOverallStatus(String overallStatus) {
        this.overallStatus = overallStatus;
    }

    public StatusModule withOverallStatus(String overallStatus) {
        this.overallStatus = overallStatus;
        return this;
    }

    @JsonProperty("expandedAccessInfo")
    public ExpandedAccessInfo getExpandedAccessInfo() {
        return expandedAccessInfo;
    }

    @JsonProperty("expandedAccessInfo")
    public void setExpandedAccessInfo(ExpandedAccessInfo expandedAccessInfo) {
        this.expandedAccessInfo = expandedAccessInfo;
    }

    public StatusModule withExpandedAccessInfo(ExpandedAccessInfo expandedAccessInfo) {
        this.expandedAccessInfo = expandedAccessInfo;
        return this;
    }

    @JsonProperty("startDateStruct")
    public StartDateStruct getStartDateStruct() {
        return startDateStruct;
    }

    @JsonProperty("startDateStruct")
    public void setStartDateStruct(StartDateStruct startDateStruct) {
        this.startDateStruct = startDateStruct;
    }

    public StatusModule withStartDateStruct(StartDateStruct startDateStruct) {
        this.startDateStruct = startDateStruct;
        return this;
    }

    @JsonProperty("completionDateStruct")
    public CompletionDateStruct getCompletionDateStruct() {
        return completionDateStruct;
    }

    @JsonProperty("completionDateStruct")
    public void setCompletionDateStruct(CompletionDateStruct completionDateStruct) {
        this.completionDateStruct = completionDateStruct;
    }

    public StatusModule withCompletionDateStruct(CompletionDateStruct completionDateStruct) {
        this.completionDateStruct = completionDateStruct;
        return this;
    }

    @JsonProperty("studyFirstSubmitQcDate")
    public String getStudyFirstSubmitQcDate() {
        return studyFirstSubmitQcDate;
    }

    @JsonProperty("studyFirstSubmitQcDate")
    public void setStudyFirstSubmitQcDate(String studyFirstSubmitQcDate) {
        this.studyFirstSubmitQcDate = studyFirstSubmitQcDate;
    }

    public StatusModule withStudyFirstSubmitQcDate(String studyFirstSubmitQcDate) {
        this.studyFirstSubmitQcDate = studyFirstSubmitQcDate;
        return this;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    public StatusModule withAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(StatusModule.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
        sb.append("primaryCompletionDateStruct");
        sb.append('=');
        sb.append(((this.primaryCompletionDateStruct == null)?"<null>":this.primaryCompletionDateStruct));
        sb.append(',');
        sb.append("statusVerifiedDate");
        sb.append('=');
        sb.append(((this.statusVerifiedDate == null)?"<null>":this.statusVerifiedDate));
        sb.append(',');
        sb.append("whyStopped");
        sb.append('=');
        sb.append(((this.whyStopped == null)?"<null>":this.whyStopped));
        sb.append(',');
        sb.append("studyFirstSubmitDate");
        sb.append('=');
        sb.append(((this.studyFirstSubmitDate == null)?"<null>":this.studyFirstSubmitDate));
        sb.append(',');
        sb.append("lastUpdateSubmitDate");
        sb.append('=');
        sb.append(((this.lastUpdateSubmitDate == null)?"<null>":this.lastUpdateSubmitDate));
        sb.append(',');
        sb.append("lastUpdatePostDateStruct");
        sb.append('=');
        sb.append(((this.lastUpdatePostDateStruct == null)?"<null>":this.lastUpdatePostDateStruct));
        sb.append(',');
        sb.append("studyFirstPostDateStruct");
        sb.append('=');
        sb.append(((this.studyFirstPostDateStruct == null)?"<null>":this.studyFirstPostDateStruct));
        sb.append(',');
        sb.append("overallStatus");
        sb.append('=');
        sb.append(((this.overallStatus == null)?"<null>":this.overallStatus));
        sb.append(',');
        sb.append("expandedAccessInfo");
        sb.append('=');
        sb.append(((this.expandedAccessInfo == null)?"<null>":this.expandedAccessInfo));
        sb.append(',');
        sb.append("startDateStruct");
        sb.append('=');
        sb.append(((this.startDateStruct == null)?"<null>":this.startDateStruct));
        sb.append(',');
        sb.append("completionDateStruct");
        sb.append('=');
        sb.append(((this.completionDateStruct == null)?"<null>":this.completionDateStruct));
        sb.append(',');
        sb.append("studyFirstSubmitQcDate");
        sb.append('=');
        sb.append(((this.studyFirstSubmitQcDate == null)?"<null>":this.studyFirstSubmitQcDate));
        sb.append(',');
        sb.append("additionalProperties");
        sb.append('=');
        sb.append(((this.additionalProperties == null)?"<null>":this.additionalProperties));
        sb.append(',');
        if (sb.charAt((sb.length()- 1)) == ',') {
            sb.setCharAt((sb.length()- 1), ']');
        } else {
            sb.append(']');
        }
        return sb.toString();
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = ((result* 31)+((this.whyStopped == null)? 0 :this.whyStopped.hashCode()));
        result = ((result* 31)+((this.lastUpdateSubmitDate == null)? 0 :this.lastUpdateSubmitDate.hashCode()));
        result = ((result* 31)+((this.studyFirstPostDateStruct == null)? 0 :this.studyFirstPostDateStruct.hashCode()));
        result = ((result* 31)+((this.expandedAccessInfo == null)? 0 :this.expandedAccessInfo.hashCode()));
        result = ((result* 31)+((this.completionDateStruct == null)? 0 :this.completionDateStruct.hashCode()));
        result = ((result* 31)+((this.studyFirstSubmitQcDate == null)? 0 :this.studyFirstSubmitQcDate.hashCode()));
        result = ((result* 31)+((this.primaryCompletionDateStruct == null)? 0 :this.primaryCompletionDateStruct.hashCode()));
        result = ((result* 31)+((this.statusVerifiedDate == null)? 0 :this.statusVerifiedDate.hashCode()));
        result = ((result* 31)+((this.studyFirstSubmitDate == null)? 0 :this.studyFirstSubmitDate.hashCode()));
        result = ((result* 31)+((this.lastUpdatePostDateStruct == null)? 0 :this.lastUpdatePostDateStruct.hashCode()));
        result = ((result* 31)+((this.additionalProperties == null)? 0 :this.additionalProperties.hashCode()));
        result = ((result* 31)+((this.overallStatus == null)? 0 :this.overallStatus.hashCode()));
        result = ((result* 31)+((this.startDateStruct == null)? 0 :this.startDateStruct.hashCode()));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof StatusModule) == false) {
            return false;
        }
        StatusModule rhs = ((StatusModule) other);
        return ((((((((((((((this.whyStopped == rhs.whyStopped)||((this.whyStopped!= null)&&this.whyStopped.equals(rhs.whyStopped)))&&((this.lastUpdateSubmitDate == rhs.lastUpdateSubmitDate)||((this.lastUpdateSubmitDate!= null)&&this.lastUpdateSubmitDate.equals(rhs.lastUpdateSubmitDate))))&&((this.studyFirstPostDateStruct == rhs.studyFirstPostDateStruct)||((this.studyFirstPostDateStruct!= null)&&this.studyFirstPostDateStruct.equals(rhs.studyFirstPostDateStruct))))&&((this.expandedAccessInfo == rhs.expandedAccessInfo)||((this.expandedAccessInfo!= null)&&this.expandedAccessInfo.equals(rhs.expandedAccessInfo))))&&((this.completionDateStruct == rhs.completionDateStruct)||((this.completionDateStruct!= null)&&this.completionDateStruct.equals(rhs.completionDateStruct))))&&((this.studyFirstSubmitQcDate == rhs.studyFirstSubmitQcDate)||((this.studyFirstSubmitQcDate!= null)&&this.studyFirstSubmitQcDate.equals(rhs.studyFirstSubmitQcDate))))&&((this.primaryCompletionDateStruct == rhs.primaryCompletionDateStruct)||((this.primaryCompletionDateStruct!= null)&&this.primaryCompletionDateStruct.equals(rhs.primaryCompletionDateStruct))))&&((this.statusVerifiedDate == rhs.statusVerifiedDate)||((this.statusVerifiedDate!= null)&&this.statusVerifiedDate.equals(rhs.statusVerifiedDate))))&&((this.studyFirstSubmitDate == rhs.studyFirstSubmitDate)||((this.studyFirstSubmitDate!= null)&&this.studyFirstSubmitDate.equals(rhs.studyFirstSubmitDate))))&&((this.lastUpdatePostDateStruct == rhs.lastUpdatePostDateStruct)||((this.lastUpdatePostDateStruct!= null)&&this.lastUpdatePostDateStruct.equals(rhs.lastUpdatePostDateStruct))))&&((this.additionalProperties == rhs.additionalProperties)||((this.additionalProperties!= null)&&this.additionalProperties.equals(rhs.additionalProperties))))&&((this.overallStatus == rhs.overallStatus)||((this.overallStatus!= null)&&this.overallStatus.equals(rhs.overallStatus))))&&((this.startDateStruct == rhs.startDateStruct)||((this.startDateStruct!= null)&&this.startDateStruct.equals(rhs.startDateStruct))));
    }

}
