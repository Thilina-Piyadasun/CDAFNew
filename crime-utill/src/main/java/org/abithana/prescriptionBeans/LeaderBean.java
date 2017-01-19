package org.abithana.prescriptionBeans;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Thilina on 1/5/2017.
 */
public class LeaderBean implements Serializable {

    private double lat;
    private double lon;
    private int LeaderTract;
    private int leaderWork;
    private List<Integer> followers=new ArrayList<>();

    public LeaderBean(double lat, double lon, int leaderTract, int leaderWork) {
        this.lat = lat;
        this.lon = lon;
        LeaderTract = leaderTract;
        this.leaderWork = leaderWork;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    public int getLeaderTract() {
        return LeaderTract;
    }

    public void setLeaderTract(int leaderTract) {
        LeaderTract = leaderTract;
    }

    public int getLeaderWork() {
        return leaderWork;
    }

    public void setLeaderWork(int leaderWork) {
        this.leaderWork = leaderWork;
    }

    public void incrementLeaderWork(int followerWork) {
        leaderWork=leaderWork+followerWork;
    }
    public List<Integer> getFollowers() {
        return followers;
    }

    public void addFollower(int follower) {
        followers.add(follower);
    }
}
