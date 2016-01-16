package com.midea.cloudSearch.thrift.server;

import org.apache.thrift.server.TServer;

public interface TServerFactory {
	
	 TServer createTServer();

}
