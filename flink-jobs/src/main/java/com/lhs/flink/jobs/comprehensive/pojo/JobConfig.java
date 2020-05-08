package com.lhs.flink.jobs.comprehensive.pojo;

public class JobConfig {

    private String jobdesc;

    public String getJobdesc() {
        return jobdesc;
    }

    public void setJobdesc(String jobdesc) {
        this.jobdesc = jobdesc;
    }

    @Override
    public String toString() {
        return "JobConfig{" +
                "jobdesc='" + jobdesc + '\'' +
                '}';
    }
}
