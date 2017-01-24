package org.abithana.prescription.impl;
import org.abithana.prescription.beans.CensusTract;

import java.util.HashSet;

/**
 * Created by malakaganga on 1/23/17.
 */
public class Cluster {

    private long clusterId;
    private HashSet<CensusTract> cencusTracts = new HashSet<CensusTract>();
    private HashSet<Long> censusIds = new HashSet<Long>();

    public HashSet<Long> getCensusIds() {
        return censusIds;
    }

    public void setCensusIds(long censusId) {
        this.censusIds.add(censusId);
    }

    public long getClusterId() {
        return clusterId;
    }

    public void setClusterId(long clusterId) {
        this.clusterId = clusterId;
    }

    public HashSet<CensusTract> getCencusTracts() {
        return cencusTracts;
    }

    public void setCencusTracts(HashSet<CensusTract> cencusTracts) {
        this.cencusTracts = cencusTracts;
    }
}