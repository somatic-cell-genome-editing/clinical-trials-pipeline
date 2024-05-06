
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
    "interventions",
    "armGroups"
})
@Generated("jsonschema2pojo")
public class ArmsInterventionsModule {

    @JsonProperty("interventions")
    private List<Intervention> interventions = new ArrayList<Intervention>();
    @JsonProperty("armGroups")
    private List<ArmGroup> armGroups = new ArrayList<ArmGroup>();
    @JsonIgnore
    private Map<String, Object> additionalProperties = new LinkedHashMap<String, Object>();

    @JsonProperty("interventions")
    public List<Intervention> getInterventions() {
        return interventions;
    }

    @JsonProperty("interventions")
    public void setInterventions(List<Intervention> interventions) {
        this.interventions = interventions;
    }

    public ArmsInterventionsModule withInterventions(List<Intervention> interventions) {
        this.interventions = interventions;
        return this;
    }

    @JsonProperty("armGroups")
    public List<ArmGroup> getArmGroups() {
        return armGroups;
    }

    @JsonProperty("armGroups")
    public void setArmGroups(List<ArmGroup> armGroups) {
        this.armGroups = armGroups;
    }

    public ArmsInterventionsModule withArmGroups(List<ArmGroup> armGroups) {
        this.armGroups = armGroups;
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

    public ArmsInterventionsModule withAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(ArmsInterventionsModule.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
        sb.append("interventions");
        sb.append('=');
        sb.append(((this.interventions == null)?"<null>":this.interventions));
        sb.append(',');
        sb.append("armGroups");
        sb.append('=');
        sb.append(((this.armGroups == null)?"<null>":this.armGroups));
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
        result = ((result* 31)+((this.interventions == null)? 0 :this.interventions.hashCode()));
        result = ((result* 31)+((this.additionalProperties == null)? 0 :this.additionalProperties.hashCode()));
        result = ((result* 31)+((this.armGroups == null)? 0 :this.armGroups.hashCode()));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof ArmsInterventionsModule) == false) {
            return false;
        }
        ArmsInterventionsModule rhs = ((ArmsInterventionsModule) other);
        return ((((this.interventions == rhs.interventions)||((this.interventions!= null)&&this.interventions.equals(rhs.interventions)))&&((this.additionalProperties == rhs.additionalProperties)||((this.additionalProperties!= null)&&this.additionalProperties.equals(rhs.additionalProperties))))&&((this.armGroups == rhs.armGroups)||((this.armGroups!= null)&&this.armGroups.equals(rhs.armGroups))));
    }

}
