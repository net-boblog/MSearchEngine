/**
 * Project Name:MSearchEngine-client-index-impl
 * File Name:CloudIndexRequest.java
 * Package Name:com.midea.cloudSearch.utils
 * Date:2016年1月18日上午11:08:27
 * Copyright (c) 2016, tanjq2@midea.com.cn All Rights Reserved.
 *
*/

package com.midea.cloudSearch.utils;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.midea.trade.common.util.FastJSONHelper;

/**
 * ClassName:CloudIndexRequest <br/>
 * Desc: 云查询索引工具类. <br/>
 * Date:     2016年1月18日 上午11:08:27 <br/>
 * @author   TANJQ2
 * @version  
 * @since    JDK 1.6
 * @see 	 
 */
public class CloudIndexUtils {

	/*clusterName:  cluster name */
	private static String clusterName = "elasticsearch";
	/*addr: ip addr for cluster master*/
	private static String addr = "10.16.69.146";
	private static int port = 9300;
	/*client:  index transport client */
	private static TransportClient client;
	
	
	private static TransportClient getClient(){
		if(client!=null){
			return client;
		}
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build();
		TransportClient client = new TransportClient(settings);
		client.addTransportAddress(new InetSocketTransportAddress(addr, port));
		return client;
	}
	
	/**
	 * exists:判断映射是否存在. <br/>
	 * @author TANJQ2
	 * @param index：映射名称
	 * @return
	 * @since JDK 1.6
	 */
	private static boolean exists(String mapping){
		IndicesExistsRequest request = new IndicesExistsRequest(mapping);
		ActionFuture<IndicesExistsResponse> res = getClient().admin().indices().exists(request);
		return res.actionGet().isExists();
	}
	/**
	 * index:索引文档接口. <br/>
	 * @author TANJQ2
	 * @param index:映射
	 * @param type：类型
	 * @param source:文档内容
	 * @return
	 * @since JDK 1.6
	 */
	public static ActionFuture<IndexResponse> index(String index,String type,XContentBuilder sourceBuilder){
		 IndexRequest request = new IndexRequest(index,type);
		 request.source(sourceBuilder);		 
		 return getClient().index(request);
	 }
	
	/**
	 * index:索引文档接口. <br/>
	 * @author TANJQ2
	 * @param index:映射
	 * @param type：类型
	 * @param id：标识 
	 * @param source:文档内容
	 * @return
	 * @since JDK 1.6
	 */
	public static ActionFuture<IndexResponse> index(String index,String type,String id,XContentBuilder sourceBuilder){
		 IndexRequest request = new IndexRequest(index,type,id);
		 request.source(sourceBuilder);
		 return getClient().index(request);
	 }
	
	/**
	 * index:获取索引文档接口. <br/>
	 * @author TANJQ2
	 * @param index:映射
	 * @param type：类型
	 * @param id：标识 
	 * @return
	 * @since JDK 1.6
	 */
	public static ActionFuture<GetResponse> getIndex(String index,String type,String id){
		GetRequest request = new GetRequest(index,type,id);
		 return getClient().get(request);
	 }
	

	/**
	 * index:删除索引文档接口. <br/>
	 * @author TANJQ2
	 * @param index:映射
	 * @param type：类型
	 * @param id：标识 
	 * @return
	 * @since JDK 1.6
	 */
	public static ActionFuture<DeleteResponse> delete(String index,String type,String id){
		DeleteRequest request = new DeleteRequest(index,type,id);
		 return getClient().delete(request);
	 }
	

	/**
	 * putMapping:创建映射接口. <br/>
	 * @author TANJQ2
	 * @param index：索引映射数组
	 * @param mappingSource：映射的json串
	 * @return
	 * @since JDK 1.6
	 */
	public static ActionFuture<PutMappingResponse> putMapping(String[] index,String type,XContentBuilder mappingBuilder){
		for(String id:index){
			if(!StringUtils.isEmpty(id) && !exists(id)){
				CreateIndexResponse res = getClient().admin().indices().prepareCreate(id).execute().actionGet();	
				System.out.println(res);
			}
		}			
		PutMappingRequest request = new PutMappingRequest(index);
		request.type(type);
		request.source(mappingBuilder);
		return getClient().admin().indices().putMapping(request);
	 }	
	
	/**
	 * putMapping:查询映射接口. <br/>
	 * @author TANJQ2
	 * @param index：索引映射数组
	 * @return
	 * @since JDK 1.6
	 */
	public static ActionFuture<GetMappingsResponse> getMapping(String[] index){
		GetMappingsRequest request = new GetMappingsRequest();
		request.indices(index);
		return getClient().admin().indices().getMappings(request);
	}
	
	public static void  main( String[] args ){
		//setMapping		
    	String[] indexes =new String[]{"test"};
    	XContentBuilder content = null;
    	try{
    	content = XContentFactory.jsonBuilder().startObject()
    	        .startObject("cloth")
    	          .startObject("properties")       
    	            .startObject("name")
    	              .field("type", "string")           
    	            .endObject()    
    	          .endObject()
    	         .endObject()
    	       .endObject();    	
    	}catch(IOException e){
    		System.out.println(e.getMessage());
    	}
		ActionFuture<PutMappingResponse> res = CloudIndexUtils.putMapping(indexes, "cloth", content);
		System.out.println(FastJSONHelper.serialize(res.actionGet()));
		
		//getMapping
		
		ActionFuture<GetMappingsResponse> getRes = CloudIndexUtils.getMapping(indexes);
		System.out.println(FastJSONHelper.serialize(getRes.actionGet().getMappings().values()));
		
		//index doc
    	XContentBuilder doc = null;
    	try{
    		doc = XContentFactory.jsonBuilder().startObject()
    	              .field("name", "t shirk")            
    	       .endObject();    	
    	}catch(IOException e){
    		System.out.println(e.getMessage());
    	}
    	ActionFuture<IndexResponse> resDoc = CloudIndexUtils.index(indexes[0], "cloth", doc);
    	System.out.println(FastJSONHelper.serialize(resDoc.actionGet()));
    	
		//get index
    	ActionFuture<GetResponse> getDoc = CloudIndexUtils.getIndex(indexes[0], "cloth","AVJT9N-SLTy24TMHvuqI");
    	System.out.println(getDoc.actionGet().getSourceAsString());
	}
}

