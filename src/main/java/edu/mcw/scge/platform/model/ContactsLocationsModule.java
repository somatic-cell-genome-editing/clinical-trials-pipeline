
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
    "locations",
    "overallOfficials"
})
@Generated("jsonschema2pojo")
public class ContactsLocationsModule {

    @JsonProperty("locations")
    private List<Location> locations = new ArrayList<Location>();
    @JsonProperty("overallOfficials")
    private List<OverallOfficial> overallOfficials = new ArrayList<OverallOfficial>();
    @JsonIgnore
    private Map<String, Object> additionalProperties = new LinkedHashMap<String, Object>();

    @JsonProperty("locations")
    public List<Location> getLocations() {
        return locations;
    }

    @JsonProperty("locations")
    public void setLocations(List<Location> locations) {
        this.locations = locations;
    }

    public ContactsLocationsModule withLocations(List<Location> locations) {
        this.locations = locations;
        return this;
    }

    @JsonProperty("overallOfficials")
    public List<OverallOfficial> getOverallOfficials() {
        return overallOfficials;
    }

    @JsonProperty("overallOfficials")
    public void setOverallOfficials(List<OverallOfficial> overallOfficials) {
        this.overallOfficials = overallOfficials;
    }

    public ContactsLocationsModule withOverallOfficials(List<OverallOfficial> overallOfficials) {
        this.overallOfficials = overallOfficials;
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

    public ContactsLocationsModule withAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(ContactsLocationsModule.class.getName()).append('@').append(Integer.toHexString(System.identityHashCode(this))).append('[');
        sb.append("locations");
        sb.append('=');
        sb.append(((this.locations == null)?"<null>":this.locations));
        sb.append(',');
        sb.append("overallOfficials");
        sb.append('=');
        sb.append(((this.overallOfficials == null)?"<null>":this.overallOfficials));
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
        result = ((result* 31)+((this.locations == null)? 0 :this.locations.hashCode()));
        result = ((result* 31)+((this.additionalProperties == null)? 0 :this.additionalProperties.hashCode()));
        result = ((result* 31)+((this.overallOfficials == null)? 0 :this.overallOfficials.hashCode()));
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof ContactsLocationsModule) == false) {
            return false;
        }
        ContactsLocationsModule rhs = ((ContactsLocationsModule) other);
        return ((((this.locations == rhs.locations)||((this.locations!= null)&&this.locations.equals(rhs.locations)))&&((this.additionalProperties == rhs.additionalProperties)||((this.additionalProperties!= null)&&this.additionalProperties.equals(rhs.additionalProperties))))&&((this.overallOfficials == rhs.overallOfficials)||((this.overallOfficials!= null)&&this.overallOfficials.equals(rhs.overallOfficials))));
    }

}
