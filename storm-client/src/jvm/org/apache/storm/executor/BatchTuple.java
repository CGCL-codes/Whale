package org.apache.storm.executor;

import org.apache.storm.tuple.Tuple;

import java.util.List;

/**
 * locate org.apache.storm.executor
 * Created by tjmaster on 18-2-1.
 */
public class BatchTuple {
    private List<Integer> outTasks;
    private Tuple tuple;

    public BatchTuple(List<Integer> outTasks, Tuple tuple) {
        this.outTasks = outTasks;
        this.tuple = tuple;
    }

    public List<Integer> getOutTasks() {
        return outTasks;
    }

    public void setOutTasks(List<Integer> outTasks) {
        this.outTasks = outTasks;
    }

    public Tuple getTuple() {
        return tuple;
    }

    public void setTuple(Tuple tuple) {
        this.tuple = tuple;
    }

    @Override
    public String toString() {
        return "BatchTuple{" +
                "outTasks=" + outTasks +
                ", tuple=" + tuple +
                '}';
    }
}
