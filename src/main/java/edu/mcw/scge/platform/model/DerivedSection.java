
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
    "miscInfoModule",
    "conditionBrowseModule"
})
@Generated("jsonschema2pojo")
public class DerivedSection {

    @JsonProperty("miscInfoModule")
    private MiscInfoModule miscInfoModule;
    @JsonProperty("conditionBrowseModule")
    private ConditionBrowseModule conditionBrowseModule;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new LinkedHashMap<String, Object>();

    @JsonProperty("miscInfoModule")
    public MiscInfoModule getMiscInfoModule() {
        return miscInfoModule;
    }

    @JsonProperty("miscInfoModule")
    public void setMiscInfoModule(MiscInfoModule miscInfoModule) {
        this.miscInfoModule = miscInfoModule;
    }

    public DerivedSection withMiscInfoModule(MiscInfoModule miscInfoModule) {
        this.miscInfoModule = miscInfoModule;
        return this;
    }

    @JsonProperty("conditionBrowseModule")
    public ConditionBrowseModule getConditionBrowseModule() {
        return conditionBrowseModule;
    }

    @JsonProperty("conditionBrowseModule")
    public void setConditionBrowseModule(ConditionBrowseModule conditionBrowseModule) {
        this.conditionBrowseModule = conditionBrowseModule;
    }

    public DerivedSection withConditionBrowseModule(ConditionBrowseModule conditionBrowseModule) {
        this.conditionBrowseModule = conditionBrowseModule;
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

    public DerivedSection withAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(DerivedSection.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
        sb.append("miscInfoModule");
        sb.append('=');
        sb.append(((this.miscInfoModule == null)?"<null>":this.miscInfoModule));
        sb.append(',');
        sb.append("conditionBrowseModule");
        sb.append('=');
        sb.append(((this.conditionBrowseModule == null)?"<null>":this.conditionBrowseModule));
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
        result = ((result* 31)+((this.conditionBrowseModule == null)? 0 :this.conditionBrowseModule.hashCode()));
        result = ((result* 31)+((this.additionalProperties == null)? 0 :this.additionalProperties.hashCode()));
        result = ((result* 31)+((this.miscInfoModule == null)? 0 :this.miscInfoModule.hashCode()));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof DerivedSection) == false) {
            return false;
        }
        DerivedSection rhs = ((DerivedSection) other);
        return ((((this.conditionBrowseModule == rhs.conditionBrowseModule)||((this.conditionBrowseModule!= null)&&this.conditionBrowseModule.equals(rhs.conditionBrowseModule)))&&((this.additionalProperties == rhs.additionalProperties)||((this.additionalProperties!= null)&&this.additionalProperties.equals(rhs.additionalProperties))))&&((this.miscInfoModule == rhs.miscInfoModule)||((this.miscInfoModule!= null)&&this.miscInfoModule.equals(rhs.miscInfoModule))));
    }

}
