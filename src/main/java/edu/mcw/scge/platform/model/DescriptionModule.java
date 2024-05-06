
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
    "briefSummary",
    "detailedDescription"
})
@Generated("jsonschema2pojo")
public class DescriptionModule {

    @JsonProperty("briefSummary")
    private String briefSummary;
    @JsonProperty("detailedDescription")
    private String detailedDescription;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new LinkedHashMap<String, Object>();

    @JsonProperty("briefSummary")
    public String getBriefSummary() {
        return briefSummary;
    }

    @JsonProperty("briefSummary")
    public void setBriefSummary(String briefSummary) {
        this.briefSummary = briefSummary;
    }

    public DescriptionModule withBriefSummary(String briefSummary) {
        this.briefSummary = briefSummary;
        return this;
    }

    @JsonProperty("detailedDescription")
    public String getDetailedDescription() {
        return detailedDescription;
    }

    @JsonProperty("detailedDescription")
    public void setDetailedDescription(String detailedDescription) {
        this.detailedDescription = detailedDescription;
    }

    public DescriptionModule withDetailedDescription(String detailedDescription) {
        this.detailedDescription = detailedDescription;
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

    public DescriptionModule withAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(DescriptionModule.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
        sb.append("briefSummary");
        sb.append('=');
        sb.append(((this.briefSummary == null)?"<null>":this.briefSummary));
        sb.append(',');
        sb.append("detailedDescription");
        sb.append('=');
        sb.append(((this.detailedDescription == null)?"<null>":this.detailedDescription));
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
        result = ((result* 31)+((this.briefSummary == null)? 0 :this.briefSummary.hashCode()));
        result = ((result* 31)+((this.detailedDescription == null)? 0 :this.detailedDescription.hashCode()));
        result = ((result* 31)+((this.additionalProperties == null)? 0 :this.additionalProperties.hashCode()));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof DescriptionModule) == false) {
            return false;
        }
        DescriptionModule rhs = ((DescriptionModule) other);
        return ((((this.briefSummary == rhs.briefSummary)||((this.briefSummary!= null)&&this.briefSummary.equals(rhs.briefSummary)))&&((this.detailedDescription == rhs.detailedDescription)||((this.detailedDescription!= null)&&this.detailedDescription.equals(rhs.detailedDescription))))&&((this.additionalProperties == rhs.additionalProperties)||((this.additionalProperties!= null)&&this.additionalProperties.equals(rhs.additionalProperties))));
    }

}
