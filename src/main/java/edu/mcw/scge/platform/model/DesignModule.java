
package edu.mcw.scge.platform.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
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
    "designInfo",
    "studyType",
    "phases",
    "enrollmentInfo"
})
@Generated("jsonschema2pojo")
public class DesignModule {

    @JsonProperty("designInfo")
    private DesignInfo designInfo;
    @JsonProperty("studyType")
    private String studyType;
    @JsonProperty("phases")
    private List<String> phases = new ArrayList<String>();
    @JsonProperty("enrollmentInfo")
    private EnrollmentInfo enrollmentInfo;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new LinkedHashMap<String, Object>();

    @JsonProperty("designInfo")
    public DesignInfo getDesignInfo() {
        return designInfo;
    }

    @JsonProperty("designInfo")
    public void setDesignInfo(DesignInfo designInfo) {
        this.designInfo = designInfo;
    }

    public DesignModule withDesignInfo(DesignInfo designInfo) {
        this.designInfo = designInfo;
        return this;
    }

    @JsonProperty("studyType")
    public String getStudyType() {
        return studyType;
    }

    @JsonProperty("studyType")
    public void setStudyType(String studyType) {
        this.studyType = studyType;
    }

    public DesignModule withStudyType(String studyType) {
        this.studyType = studyType;
        return this;
    }

    @JsonProperty("phases")
    public List<String> getPhases() {
        return phases;
    }

    @JsonProperty("phases")
    public void setPhases(List<String> phases) {
        this.phases = phases;
    }

    public DesignModule withPhases(List<String> phases) {
        this.phases = phases;
        return this;
    }

    @JsonProperty("enrollmentInfo")
    public EnrollmentInfo getEnrollmentInfo() {
        return enrollmentInfo;
    }

    @JsonProperty("enrollmentInfo")
    public void setEnrollmentInfo(EnrollmentInfo enrollmentInfo) {
        this.enrollmentInfo = enrollmentInfo;
    }

    public DesignModule withEnrollmentInfo(EnrollmentInfo enrollmentInfo) {
        this.enrollmentInfo = enrollmentInfo;
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

    public DesignModule withAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(DesignModule.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
        sb.append("designInfo");
        sb.append('=');
        sb.append(((this.designInfo == null)?"<null>":this.designInfo));
        sb.append(',');
        sb.append("studyType");
        sb.append('=');
        sb.append(((this.studyType == null)?"<null>":this.studyType));
        sb.append(',');
        sb.append("phases");
        sb.append('=');
        sb.append(((this.phases == null)?"<null>":this.phases));
        sb.append(',');
        sb.append("enrollmentInfo");
        sb.append('=');
        sb.append(((this.enrollmentInfo == null)?"<null>":this.enrollmentInfo));
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
        result = ((result* 31)+((this.additionalProperties == null)? 0 :this.additionalProperties.hashCode()));
        result = ((result* 31)+((this.designInfo == null)? 0 :this.designInfo.hashCode()));
        result = ((result* 31)+((this.studyType == null)? 0 :this.studyType.hashCode()));
        result = ((result* 31)+((this.phases == null)? 0 :this.phases.hashCode()));
        result = ((result* 31)+((this.enrollmentInfo == null)? 0 :this.enrollmentInfo.hashCode()));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof DesignModule) == false) {
            return false;
        }
        DesignModule rhs = ((DesignModule) other);
        return ((((((this.additionalProperties == rhs.additionalProperties)||((this.additionalProperties!= null)&&this.additionalProperties.equals(rhs.additionalProperties)))&&((this.designInfo == rhs.designInfo)||((this.designInfo!= null)&&this.designInfo.equals(rhs.designInfo))))&&((this.studyType == rhs.studyType)||((this.studyType!= null)&&this.studyType.equals(rhs.studyType))))&&((this.phases == rhs.phases)||((this.phases!= null)&&this.phases.equals(rhs.phases))))&&((this.enrollmentInfo == rhs.enrollmentInfo)||((this.enrollmentInfo!= null)&&this.enrollmentInfo.equals(rhs.enrollmentInfo))));
    }

}
