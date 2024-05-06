
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
    "sex",
    "minimumAge",
    "stdAges",
    "eligibilityCriteria",
    "healthyVolunteers",
    "maximumAge"
})
@Generated("jsonschema2pojo")
public class EligibilityModule {

    @JsonProperty("sex")
    private String sex;
    @JsonProperty("minimumAge")
    private String minimumAge;
    @JsonProperty("stdAges")
    private List<String> stdAges = new ArrayList<String>();
    @JsonProperty("eligibilityCriteria")
    private String eligibilityCriteria;
    @JsonProperty("healthyVolunteers")
    private Boolean healthyVolunteers;
    @JsonProperty("maximumAge")
    private String maximumAge;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new LinkedHashMap<String, Object>();

    @JsonProperty("sex")
    public String getSex() {
        return sex;
    }

    @JsonProperty("sex")
    public void setSex(String sex) {
        this.sex = sex;
    }

    public EligibilityModule withSex(String sex) {
        this.sex = sex;
        return this;
    }

    @JsonProperty("minimumAge")
    public String getMinimumAge() {
        return minimumAge;
    }

    @JsonProperty("minimumAge")
    public void setMinimumAge(String minimumAge) {
        this.minimumAge = minimumAge;
    }

    public EligibilityModule withMinimumAge(String minimumAge) {
        this.minimumAge = minimumAge;
        return this;
    }

    @JsonProperty("stdAges")
    public List<String> getStdAges() {
        return stdAges;
    }

    @JsonProperty("stdAges")
    public void setStdAges(List<String> stdAges) {
        this.stdAges = stdAges;
    }

    public EligibilityModule withStdAges(List<String> stdAges) {
        this.stdAges = stdAges;
        return this;
    }

    @JsonProperty("eligibilityCriteria")
    public String getEligibilityCriteria() {
        return eligibilityCriteria;
    }

    @JsonProperty("eligibilityCriteria")
    public void setEligibilityCriteria(String eligibilityCriteria) {
        this.eligibilityCriteria = eligibilityCriteria;
    }

    public EligibilityModule withEligibilityCriteria(String eligibilityCriteria) {
        this.eligibilityCriteria = eligibilityCriteria;
        return this;
    }

    @JsonProperty("healthyVolunteers")
    public Boolean getHealthyVolunteers() {
        return healthyVolunteers;
    }

    @JsonProperty("healthyVolunteers")
    public void setHealthyVolunteers(Boolean healthyVolunteers) {
        this.healthyVolunteers = healthyVolunteers;
    }

    public EligibilityModule withHealthyVolunteers(Boolean healthyVolunteers) {
        this.healthyVolunteers = healthyVolunteers;
        return this;
    }

    @JsonProperty("maximumAge")
    public String getMaximumAge() {
        return maximumAge;
    }

    @JsonProperty("maximumAge")
    public void setMaximumAge(String maximumAge) {
        this.maximumAge = maximumAge;
    }

    public EligibilityModule withMaximumAge(String maximumAge) {
        this.maximumAge = maximumAge;
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

    public EligibilityModule withAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(EligibilityModule.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
        sb.append("sex");
        sb.append('=');
        sb.append(((this.sex == null)?"<null>":this.sex));
        sb.append(',');
        sb.append("minimumAge");
        sb.append('=');
        sb.append(((this.minimumAge == null)?"<null>":this.minimumAge));
        sb.append(',');
        sb.append("stdAges");
        sb.append('=');
        sb.append(((this.stdAges == null)?"<null>":this.stdAges));
        sb.append(',');
        sb.append("eligibilityCriteria");
        sb.append('=');
        sb.append(((this.eligibilityCriteria == null)?"<null>":this.eligibilityCriteria));
        sb.append(',');
        sb.append("healthyVolunteers");
        sb.append('=');
        sb.append(((this.healthyVolunteers == null)?"<null>":this.healthyVolunteers));
        sb.append(',');
        sb.append("maximumAge");
        sb.append('=');
        sb.append(((this.maximumAge == null)?"<null>":this.maximumAge));
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
        result = ((result* 31)+((this.sex == null)? 0 :this.sex.hashCode()));
        result = ((result* 31)+((this.minimumAge == null)? 0 :this.minimumAge.hashCode()));
        result = ((result* 31)+((this.stdAges == null)? 0 :this.stdAges.hashCode()));
        result = ((result* 31)+((this.eligibilityCriteria == null)? 0 :this.eligibilityCriteria.hashCode()));
        result = ((result* 31)+((this.healthyVolunteers == null)? 0 :this.healthyVolunteers.hashCode()));
        result = ((result* 31)+((this.additionalProperties == null)? 0 :this.additionalProperties.hashCode()));
        result = ((result* 31)+((this.maximumAge == null)? 0 :this.maximumAge.hashCode()));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof EligibilityModule) == false) {
            return false;
        }
        EligibilityModule rhs = ((EligibilityModule) other);
        return ((((((((this.sex == rhs.sex)||((this.sex!= null)&&this.sex.equals(rhs.sex)))&&((this.minimumAge == rhs.minimumAge)||((this.minimumAge!= null)&&this.minimumAge.equals(rhs.minimumAge))))&&((this.stdAges == rhs.stdAges)||((this.stdAges!= null)&&this.stdAges.equals(rhs.stdAges))))&&((this.eligibilityCriteria == rhs.eligibilityCriteria)||((this.eligibilityCriteria!= null)&&this.eligibilityCriteria.equals(rhs.eligibilityCriteria))))&&((this.healthyVolunteers == rhs.healthyVolunteers)||((this.healthyVolunteers!= null)&&this.healthyVolunteers.equals(rhs.healthyVolunteers))))&&((this.additionalProperties == rhs.additionalProperties)||((this.additionalProperties!= null)&&this.additionalProperties.equals(rhs.additionalProperties))))&&((this.maximumAge == rhs.maximumAge)||((this.maximumAge!= null)&&this.maximumAge.equals(rhs.maximumAge))));
    }

}
