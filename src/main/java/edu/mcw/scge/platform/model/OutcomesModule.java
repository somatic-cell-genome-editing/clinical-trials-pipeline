
package edu.mcw.scge.platform.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
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
    "secondaryOutcomes",
    "primaryOutcomes"
})
@Generated("jsonschema2pojo")
public class OutcomesModule {

    @JsonProperty("secondaryOutcomes")
    private List<SecondaryOutcome> secondaryOutcomes = new ArrayList<SecondaryOutcome>();
    @JsonProperty("primaryOutcomes")
    private List<PrimaryOutcome> primaryOutcomes = new ArrayList<PrimaryOutcome>();
    @JsonIgnore
    private Map<String, Object> additionalProperties = new LinkedHashMap<String, Object>();

    @JsonProperty("secondaryOutcomes")
    public List<SecondaryOutcome> getSecondaryOutcomes() {
        return secondaryOutcomes;
    }

    @JsonProperty("secondaryOutcomes")
    public void setSecondaryOutcomes(List<SecondaryOutcome> secondaryOutcomes) {
        this.secondaryOutcomes = secondaryOutcomes;
    }

    public OutcomesModule withSecondaryOutcomes(List<SecondaryOutcome> secondaryOutcomes) {
        this.secondaryOutcomes = secondaryOutcomes;
        return this;
    }

    @JsonProperty("primaryOutcomes")
    public List<PrimaryOutcome> getPrimaryOutcomes() {
        return primaryOutcomes;
    }

    @JsonProperty("primaryOutcomes")
    public void setPrimaryOutcomes(List<PrimaryOutcome> primaryOutcomes) {
        this.primaryOutcomes = primaryOutcomes;
    }

    public OutcomesModule withPrimaryOutcomes(List<PrimaryOutcome> primaryOutcomes) {
        this.primaryOutcomes = primaryOutcomes;
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

    public OutcomesModule withAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(OutcomesModule.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
        sb.append("secondaryOutcomes");
        sb.append('=');
        sb.append(((this.secondaryOutcomes == null)?"<null>":this.secondaryOutcomes));
        sb.append(',');
        sb.append("primaryOutcomes");
        sb.append('=');
        sb.append(((this.primaryOutcomes == null)?"<null>":this.primaryOutcomes));
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
        result = ((result* 31)+((this.secondaryOutcomes == null)? 0 :this.secondaryOutcomes.hashCode()));
        result = ((result* 31)+((this.additionalProperties == null)? 0 :this.additionalProperties.hashCode()));
        result = ((result* 31)+((this.primaryOutcomes == null)? 0 :this.primaryOutcomes.hashCode()));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof OutcomesModule) == false) {
            return false;
        }
        OutcomesModule rhs = ((OutcomesModule) other);
        return ((((this.secondaryOutcomes == rhs.secondaryOutcomes)||((this.secondaryOutcomes!= null)&&this.secondaryOutcomes.equals(rhs.secondaryOutcomes)))&&((this.additionalProperties == rhs.additionalProperties)||((this.additionalProperties!= null)&&this.additionalProperties.equals(rhs.additionalProperties))))&&((this.primaryOutcomes == rhs.primaryOutcomes)||((this.primaryOutcomes!= null)&&this.primaryOutcomes.equals(rhs.primaryOutcomes))));
    }

}
