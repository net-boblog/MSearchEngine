package com.midea.cloudSearch.thrift.server;
import java.util.Map;
import java.util.concurrent.Executors;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import com.midea.cloudSearch.exception.ThriftInitException;


public class DefaultTServerFactory implements TServerFactory {
	private final Map<String,TProcessor> map;
	
	
	public DefaultTServerFactory(Map<String, TProcessor> processorMap) {
	    this.map = processorMap;
	}

	@Override
	public TServer createTServer() {
		TMultiplexedProcessor tMultiplexedProcessor = new TMultiplexedProcessor();
		
		 for (Map.Entry<String, TProcessor> entry : this.map.entrySet()) {
			   tMultiplexedProcessor.registerProcessor(entry.getKey(), entry.getValue());
		 }
		TNonblockingServerSocket socket = null;
		try {
			socket = new TNonblockingServerSocket(5000);
		} catch (TTransportException  e) {
			throw new ThriftInitException(e.getMessage(), e);
		}

		TThreadedSelectorServer.Args args = new TThreadedSelectorServer.Args(socket);
		args.executorService(Executors.newFixedThreadPool(20));
		args.protocolFactory(new TCompactProtocol.Factory());
		args.processor(tMultiplexedProcessor);
		args.transportFactory(new TFramedTransport.Factory());
		TThreadedSelectorServer server = new TThreadedSelectorServer(args);
        return server;
	}

}
