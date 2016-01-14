package com.midea.cloudSearch.thrift.stub;
import java.io.IOException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import com.midea.cloudSearch.thrift.gen.*;




public class BlackServiceStub implements BlackService.Iface{
	
	private final BlackService.Client client ;
	
	public BlackServiceStub(String host,int port) throws IOException, TTransportException{
		TFramedTransport transport = new TFramedTransport(new TSocket(host,port));
		transport.open();
        client = new BlackService.Client(new TMultiplexedProtocol(new TCompactProtocol(transport), "blackService"));
	}

	@Override
	public boolean isBlack(int uid) throws TException {
		return client.isBlack(uid);
	}
	
	
	  /**
	   * thrift 客户端调用demo
	   * @param args
	   * @throws IOException
	   * @throws TException
	   * @throws InterruptedException
	   */
	  public static void main( String[] args ) throws IOException, TException, InterruptedException
	    {   
	        final BlackServiceStub blackServiceStub = new BlackServiceStub("127.0.0.1",5000);
	        System.out.println(blackServiceStub.isBlack(11));
	        Thread.sleep(Integer.MAX_VALUE);
	    }

}
