package com.midea.cloudSearch.thrift.server;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.thrift.TProcessor;
import org.apache.thrift.server.TServer;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.midea.cloudSearch.thrift.gen.BlackService;
import com.midea.cloudSearch.thrift.server.impl.BlackServiceImpl;


@Component
@Scope("singleton")
public class ThriftServer {
	
	@PostConstruct
	public void  start(){
		final Map<String, TProcessor> processorMap = new HashMap<String, TProcessor>();
		processorMap.put("blackService",new BlackService.Processor<BlackService.Iface>(new BlackServiceImpl()));
		TServerFactory _tServerFactory  = new DefaultTServerFactory(processorMap);
		TServer server = _tServerFactory.createTServer();
		server.serve();
	}

}
