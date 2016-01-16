package com.midea.cloudSearch.thrift.server.impl;
import org.apache.thrift.TException;
import com.midea.cloudSearch.thrift.gen.*;

public class BlackServiceImpl implements BlackService.Iface {

	@Override
	public boolean isBlack(int uid) throws TException {
		return true;
	}

}
