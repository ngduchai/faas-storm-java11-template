package com.openfaas.function;

import java.util.Collection;
import java.util.List;

import org.apache.storm.task.IOutputCollector;
import org.apache.storm.tuple.Tuple;

public class FaaSOutputCollector implements IOutputCollector {

    CallbackMessage _msg;
    FaasInvoker invoker;

    public FaaSOutputCollector() {
        
    }

    @Override
    public void reportError(Throwable error) {
        _msg.addError(error);
    }

    @Override
    public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        // Call function accordingly
        List<Integer> taskIds = null;
        // Then record for tracking back
        _msg.trackEmission(null, streamId, anchors, tuple);
        return taskIds;
    }

    @Override
    public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        // Call function directly

        // Record for back tracking
        _msg.trackEmission(taskId, streamId, anchors, tuple);
    }

    @Override
    public void ack(Tuple input) {
        _msg.addAck(input);
    }

    @Override
    public void fail(Tuple input) {
        _msg.addFail(input);
    }

    @Override
    public void resetTimeout(Tuple input) {
       _msg.resetTimeout(input);
    }

    public CallbackMessage getCallbackMessage() {
        return this._msg;
    }

    public void clean() {
        this._msg.clean();
    }

}
