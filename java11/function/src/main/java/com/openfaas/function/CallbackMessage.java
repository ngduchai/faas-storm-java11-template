package com.openfaas.function;

import java.util.Collection;
import java.util.List;

import org.apache.storm.tuple.Tuple;

public class CallbackMessage {

    public class TupleEmissionTracker {
        Integer taskId;
        String streamId;
        Collection<Tuple> anchors;
        List<Object> tuple;
        
        public TupleEmissionTracker(Integer taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
            this.taskId = taskId;
            this.streamId = streamId;
            this.anchors = anchors;
            this.tuple = tuple;
        }
        
    }

    private List<Throwable> errors;
    private List<Tuple> acks;
    private List<Tuple> fails;
    private List<Tuple> resetTimeouts;
    private List<TupleEmissionTracker> emissionTrackers;

    public void addAck(Tuple tuple) {
        acks.add(tuple);
    }

    public void addFail(Tuple tuple) {
        fails.add(tuple);
    }

    public void addError(Throwable error) {
        errors.add(error);
    }

    public void resetTimeout(Tuple tuple) {
        resetTimeouts.add(tuple);
    }

    public List<Throwable> getErrors() {
        return errors;
    }

    public List<Tuple> getAcks() {
        return acks;
    }

    public List<Tuple> getFails() {
        return fails;
    }

    public List<Tuple> getResetTimeout() {
        return resetTimeouts;
    }
    
    public void trackEmission(Integer taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        this.emissionTrackers.add(new TupleEmissionTracker(taskId, streamId, anchors, tuple));
    }

    public String dump() {
        return this.toString();
    }

    public void clean() {
        errors.clear();
        acks.clear();
        fails.clear();
        resetTimeouts.clear();
        emissionTrackers.clear();
    }

}