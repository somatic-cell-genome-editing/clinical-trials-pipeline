
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
    "hasResults",
    "protocolSection",
    "derivedSection"
})
@Generated("jsonschema2pojo")
public class Study {

    @JsonProperty("hasResults")
    private Boolean hasResults;
    @JsonProperty("protocolSection")
    private ProtocolSection protocolSection;
    @JsonProperty("derivedSection")
    private DerivedSection derivedSection;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new LinkedHashMap<String, Object>();

    @JsonProperty("hasResults")
    public Boolean getHasResults() {
        return hasResults;
    }

    @JsonProperty("hasResults")
    public void setHasResults(Boolean hasResults) {
        this.hasResults = hasResults;
    }

    public Study withHasResults(Boolean hasResults) {
        this.hasResults = hasResults;
        return this;
    }

    @JsonProperty("protocolSection")
    public ProtocolSection getProtocolSection() {
        return protocolSection;
    }

    @JsonProperty("protocolSection")
    public void setProtocolSection(ProtocolSection protocolSection) {
        this.protocolSection = protocolSection;
    }

    public Study withProtocolSection(ProtocolSection protocolSection) {
        this.protocolSection = protocolSection;
        return this;
    }

    @JsonProperty("derivedSection")
    public DerivedSection getDerivedSection() {
        return derivedSection;
    }

    @JsonProperty("derivedSection")
    public void setDerivedSection(DerivedSection derivedSection) {
        this.derivedSection = derivedSection;
    }

    public Study withDerivedSection(DerivedSection derivedSection) {
        this.derivedSection = derivedSection;
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

    public Study withAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(Study.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
        sb.append("hasResults");
        sb.append('=');
        sb.append(((this.hasResults == null)?"<null>":this.hasResults));
        sb.append(',');
        sb.append("protocolSection");
        sb.append('=');
        sb.append(((this.protocolSection == null)?"<null>":this.protocolSection));
        sb.append(',');
        sb.append("derivedSection");
        sb.append('=');
        sb.append(((this.derivedSection == null)?"<null>":this.derivedSection));
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
        result = ((result* 31)+((this.protocolSection == null)? 0 :this.protocolSection.hashCode()));
        result = ((result* 31)+((this.hasResults == null)? 0 :this.hasResults.hashCode()));
        result = ((result* 31)+((this.additionalProperties == null)? 0 :this.additionalProperties.hashCode()));
        result = ((result* 31)+((this.derivedSection == null)? 0 :this.derivedSection.hashCode()));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof Study) == false) {
            return false;
        }
        Study rhs = ((Study) other);
        return (((((this.protocolSection == rhs.protocolSection)||((this.protocolSection!= null)&&this.protocolSection.equals(rhs.protocolSection)))&&((this.hasResults == rhs.hasResults)||((this.hasResults!= null)&&this.hasResults.equals(rhs.hasResults))))&&((this.additionalProperties == rhs.additionalProperties)||((this.additionalProperties!= null)&&this.additionalProperties.equals(rhs.additionalProperties))))&&((this.derivedSection == rhs.derivedSection)||((this.derivedSection!= null)&&this.derivedSection.equals(rhs.derivedSection))));
    }

}
