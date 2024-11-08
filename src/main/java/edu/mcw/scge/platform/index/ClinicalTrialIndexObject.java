package edu.mcw.scge.platform.index;

import edu.mcw.scge.datamodel.ClinicalTrialExternalLink;
import edu.mcw.scge.datamodel.ClinicalTrialRecord;

import java.util.List;
import java.util.Set;

public class ClinicalTrialIndexObject extends ClinicalTrialRecord {

    private Set<String> phases;
    private Set<String> status;
    private Set<String> standardAges;
    private Set<String> locations;
    private String category;

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public Set<String> getPhases() {
        return phases;
    }

    public void setPhases(Set<String> phases) {
        this.phases = phases;
    }

    public Set<String> getStatus() {
        return status;
    }

    public void setStatus(Set<String> status) {
        this.status = status;
    }

    public Set<String> getStandardAges() {
        return standardAges;
    }

    public void setStandardAges(Set<String> standardAges) {
        this.standardAges = standardAges;
    }

    public Set<String> getLocations() {
        return locations;
    }

    public void setLocations(Set<String> locations) {
        this.locations = locations;
    }
}
