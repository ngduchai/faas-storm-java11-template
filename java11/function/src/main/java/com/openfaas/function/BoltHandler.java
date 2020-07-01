package com.openfaas.function;

import com.openfaas.model.IHandler;
import com.openfaas.model.IResponse;
import com.openfaas.model.IRequest;
import com.openfaas.model.Response;

import org.apache.storm.task.IBolt;
import org.apache.storm.tuple.Tuple;

public class BoltHandler implements com.openfaas.model.IHandler {

    private IBolt bolt;
    private FaaSOutputCollector collector;

    public BoltHandler(IBolt bolt, FaaSOutputCollector collector) {
        this.bolt = bolt;
        this.collector = collector;
    }

    public IResponse Handle(IRequest req) {

        Tuple tuple = TupleSerializer.loadTuple(req.getBody());
        bolt.execute(tuple);
        Response res = new Response();
	    res.setBody(collector.getCallbackMessage().dump());

	    return res;
    }
}
