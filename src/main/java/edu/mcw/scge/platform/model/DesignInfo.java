
package edu.mcw.scge.platform.model;

import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Generated;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "allocation",
    "primaryPurpose",
    "interventionModel",
    "maskingInfo",
    "interventionModelDescription"
})
@Generated("jsonschema2pojo")
public class DesignInfo {

    @JsonProperty("allocation")
    private String allocation;
    @JsonProperty("primaryPurpose")
    private String primaryPurpose;
    @JsonProperty("interventionModel")
    private String interventionModel;
    @JsonProperty("maskingInfo")
    private MaskingInfo maskingInfo;
    @JsonProperty("interventionModelDescription")
    private String interventionModelDescription;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new LinkedHashMap<String, Object>();

    @JsonProperty("allocation")
    public String getAllocation() {
        return allocation;
    }

    @JsonProperty("allocation")
    public void setAllocation(String allocation) {
        this.allocation = allocation;
    }

    public DesignInfo withAllocation(String allocation) {
        this.allocation = allocation;
        return this;
    }

    @JsonProperty("primaryPurpose")
    public String getPrimaryPurpose() {
        return primaryPurpose;
    }

    @JsonProperty("primaryPurpose")
    public void setPrimaryPurpose(String primaryPurpose) {
        this.primaryPurpose = primaryPurpose;
    }

    public DesignInfo withPrimaryPurpose(String primaryPurpose) {
        this.primaryPurpose = primaryPurpose;
        return this;
    }

    @JsonProperty("interventionModel")
    public String getInterventionModel() {
        return interventionModel;
    }

    @JsonProperty("interventionModel")
    public void setInterventionModel(String interventionModel) {
        this.interventionModel = interventionModel;
    }

    public DesignInfo withInterventionModel(String interventionModel) {
        this.interventionModel = interventionModel;
        return this;
    }

    @JsonProperty("maskingInfo")
    public MaskingInfo getMaskingInfo() {
        return maskingInfo;
    }

    @JsonProperty("maskingInfo")
    public void setMaskingInfo(MaskingInfo maskingInfo) {
        this.maskingInfo = maskingInfo;
    }

    public DesignInfo withMaskingInfo(MaskingInfo maskingInfo) {
        this.maskingInfo = maskingInfo;
        return this;
    }

    @JsonProperty("interventionModelDescription")
    public String getInterventionModelDescription() {
        return interventionModelDescription;
    }

    @JsonProperty("interventionModelDescription")
    public void setInterventionModelDescription(String interventionModelDescription) {
        this.interventionModelDescription = interventionModelDescription;
    }

    public DesignInfo withInterventionModelDescription(String interventionModelDescription) {
        this.interventionModelDescription = interventionModelDescription;
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

    public DesignInfo withAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(DesignInfo.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
        sb.append("allocation");
        sb.append('=');
        sb.append(((this.allocation == null)?"<null>":this.allocation));
        sb.append(',');
        sb.append("primaryPurpose");
        sb.append('=');
        sb.append(((this.primaryPurpose == null)?"<null>":this.primaryPurpose));
        sb.append(',');
        sb.append("interventionModel");
        sb.append('=');
        sb.append(((this.interventionModel == null)?"<null>":this.interventionModel));
        sb.append(',');
        sb.append("maskingInfo");
        sb.append('=');
        sb.append(((this.maskingInfo == null)?"<null>":this.maskingInfo));
        sb.append(',');
        sb.append("interventionModelDescription");
        sb.append('=');
        sb.append(((this.interventionModelDescription == null)?"<null>":this.interventionModelDescription));
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
        result = ((result* 31)+((this.allocation == null)? 0 :this.allocation.hashCode()));
        result = ((result* 31)+((this.primaryPurpose == null)? 0 :this.primaryPurpose.hashCode()));
        result = ((result* 31)+((this.interventionModel == null)? 0 :this.interventionModel.hashCode()));
        result = ((result* 31)+((this.maskingInfo == null)? 0 :this.maskingInfo.hashCode()));
        result = ((result* 31)+((this.interventionModelDescription == null)? 0 :this.interventionModelDescription.hashCode()));
        result = ((result* 31)+((this.additionalProperties == null)? 0 :this.additionalProperties.hashCode()));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof DesignInfo) == false) {
            return false;
        }
        DesignInfo rhs = ((DesignInfo) other);
        return (((((((this.allocation == rhs.allocation)||((this.allocation!= null)&&this.allocation.equals(rhs.allocation)))&&((this.primaryPurpose == rhs.primaryPurpose)||((this.primaryPurpose!= null)&&this.primaryPurpose.equals(rhs.primaryPurpose))))&&((this.interventionModel == rhs.interventionModel)||((this.interventionModel!= null)&&this.interventionModel.equals(rhs.interventionModel))))&&((this.maskingInfo == rhs.maskingInfo)||((this.maskingInfo!= null)&&this.maskingInfo.equals(rhs.maskingInfo))))&&((this.interventionModelDescription == rhs.interventionModelDescription)||((this.interventionModelDescription!= null)&&this.interventionModelDescription.equals(rhs.interventionModelDescription))))&&((this.additionalProperties == rhs.additionalProperties)||((this.additionalProperties!= null)&&this.additionalProperties.equals(rhs.additionalProperties))));
    }

}
