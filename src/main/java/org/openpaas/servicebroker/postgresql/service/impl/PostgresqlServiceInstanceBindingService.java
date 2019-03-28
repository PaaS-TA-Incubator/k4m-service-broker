package org.openpaas.servicebroker.postgresql.service.impl;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.openpaas.servicebroker.exception.ServiceBrokerException;
import org.openpaas.servicebroker.exception.ServiceInstanceBindingExistsException;
import org.openpaas.servicebroker.model.CreateServiceInstanceBindingRequest;
import org.openpaas.servicebroker.model.DeleteServiceInstanceBindingRequest;
import org.openpaas.servicebroker.model.ServiceInstance;
import org.openpaas.servicebroker.model.ServiceInstanceBinding;
import org.openpaas.servicebroker.service.ServiceInstanceBindingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

/**
 * 서비스 인스턴스 바인딩 관련 서비스가 제공해야하는 메소드를 정의한 인터페이스 클래스인 ServiceInstanceBindingService를 상속하여
 *  Postgresql 서비스 인스턴스 바인딩  관련 메소드를 구현한 클래스. 
 *  서비스 인스턴스 바인드 생성/삭제/조회 를 구현한다.
 *  
 */
@PropertySource("classpath:datasource.properties")
@Service
public class PostgresqlServiceInstanceBindingService implements ServiceInstanceBindingService {

	private static final Logger logger = LoggerFactory.getLogger(PostgresqlServiceInstanceBindingService.class);
	
	//@Value("${jdbc.host}")
	//private String jdbc_host;
	
	@Autowired
	private Environment env;
	
	@Autowired
	private PostgresqlAdminService postgresqlAdminService; 
	
	
	@Autowired
	public PostgresqlServiceInstanceBindingService(PostgresqlAdminService postgresqlAdminService) {
		this.postgresqlAdminService = postgresqlAdminService;
	}
	
	/**
	 * Binding(create)
	 */
	@Override
	public ServiceInstanceBinding createServiceInstanceBinding(
			CreateServiceInstanceBindingRequest request)
			throws ServiceInstanceBindingExistsException, ServiceBrokerException {
		
		logger.debug("PostgresqlServiceInstanceBindingService CLASS createServiceInstanceBinding");
		
		ServiceInstanceBinding findBinding = postgresqlAdminService.findBindById(request.getBindingId());
		
		// 요청 정보로부터 ServiceInstanceBinding 정보를 생성합니다.
		ServiceInstanceBinding binding = postgresqlAdminService.createServiceInstanceBindingByRequest(request);
		
		/* 요청 정보롭터 ServiceInstanceBinding 정보를 생성 할 경우 다음 처리부분이 불필요하여 주석처리 합니다.
		if (binding != null) {
			throw new ServiceInstanceBindingExistsException(binding);
		}
		*/
		if(findBinding != null){
			if(findBinding.getServiceInstanceId().equals(binding.getServiceInstanceId()) &&
					findBinding.getId().equals(binding.getId())  &&
					findBinding.getAppGuid().equals(binding.getAppGuid())){
				findBinding = getBindingInfo(request, findBinding);
				findBinding.setHttpStatusOK();
				return findBinding;
			}else{
				throw new ServiceInstanceBindingExistsException(binding);
			}
			
		}
		
		// 요청 정보로부터 ServiceInstance정보를 조회합니다.
		ServiceInstance instance = postgresqlAdminService.findById(request.getServiceInstanceId());
					
		// ServiceInstance정보가 없을 경우 예외처리
		if(instance == null) throw new ServiceBrokerException("Not Exists ServiceInstance");

		// 사용자 아이디를 생성합니다.
		String username = postgresqlAdminService.getUsername(request.getBindingId());

		// Database명을 조회합니다.
		//String database = instance.getServiceInstanceId();
		String database = username;
		
		// 사용자 비밀번호를 생성합니다.
		//String password = UUID.randomUUID().toString().replace("-", "");
		String password = postgresqlAdminService.getUsername(request.getServiceInstanceId());
		
		/* 새로운 사용자명이 존재하는지 검증.*/
		if (postgresqlAdminService.isExistsUser(username)) {
			postgresqlAdminService.deleteUser(request.getBindingId());
		}
		
		/*if(postgresqlAdminService.checkUserConnections(instance.getPlanId(), instance.getServiceInstanceId())){
			throw new ServiceBrokerException("It may not exceed the specified plan.(Not assign Max User Connection)");
		}*/
		// 새로운 사용자를 생성합니다.
		postgresqlAdminService.createUser(database, username, password);
		
		// 반환될 credentials 정보를 생성합니다.
		Map<String,Object> credentials = new HashMap<String,Object>();
		credentials.put("name", database);
		credentials.put("hostname", env.getRequiredProperty("jdbc.host"));
		credentials.put("port", env.getRequiredProperty("jdbc.port"));
		credentials.put("username", username);
		credentials.put("password", password);
		credentials.put("uri", postgresqlAdminService.getConnectionString(database, username, password, env.getRequiredProperty("jdbc.host"),env.getRequiredProperty("jdbc.port")));
		binding = new ServiceInstanceBinding(request.getBindingId(), instance.getServiceInstanceId(), credentials, null, request.getAppGuid());
		
		// Binding 정보를 저장합니다.
		postgresqlAdminService.saveBind(binding);
		
		// ServiceInstance의 Plan에 따라 사용자별 MAX_USER_CONNECTIONS 정보를 조정합니다.
		//postgresqlAdminService.setUserConnections(instance.getPlanId(), instance.getServiceInstanceId());
		
		return binding;
	}

	/**
	 * Binding(delete)
	 */
	@Override
	public ServiceInstanceBinding deleteServiceInstanceBinding(DeleteServiceInstanceBindingRequest request)
			throws ServiceBrokerException {
		
		String bindingId = request.getBindingId();
		
		// ServiceInstanceBinding 정보를 조회합니다.
		ServiceInstanceBinding binding = postgresqlAdminService.findBindById(bindingId);
		if(binding ==  null) return null;
		
		// ServiceInstance 정보를 조회합니다.
		ServiceInstance instance = postgresqlAdminService.findById(binding.getServiceInstanceId());
		if(instance ==  null) return null;
		
		// bindingId로 사용자를 삭제합니다.
		postgresqlAdminService.deleteUser(bindingId);
		
		// bindingId로 Binding 정보를 삭제합니다.
		postgresqlAdminService.deleteBind(bindingId);
		
		// 사용자 삭제휴 ServiceInstance Plan을 기준으로 사용자별 MAX_USER_CONNECTIONS 정보를 조정합니다.
		//postgresqlAdminService.setUserConnections(instance.getPlanId(), instance.getServiceInstanceId());
		
		return binding;
	}
	
	/**
	 * Binding Info
	 */
	protected ServiceInstanceBinding getServiceInstanceBinding(String id) {
		return postgresqlAdminService.findBindById(id);
	}
	
	/**
	 * Binding Info
	 * @param request
	 * @param instance
	 * @return
	 */
	public ServiceInstanceBinding getBindingInfo(CreateServiceInstanceBindingRequest request, ServiceInstanceBinding instance){
		// 사용자 아이디를 생성합니다.
		String username = postgresqlAdminService.getUsername(request.getBindingId());
		
		// Database명을 조회합니다.
		String database = username;
				
		// 사용자 비밀번호를 생성합니다.
		//String password = UUID.randomUUID().toString().replace("-", "");
		String password = postgresqlAdminService.getUsername(request.getServiceInstanceId());
		
		// 반환될 credentials 정보를 생성합니다.
		Map<String,Object> credentials = new HashMap<String,Object>();
		credentials.put("name", database);
		credentials.put("hostname", env.getRequiredProperty("jdbc.host"));
		credentials.put("port", env.getRequiredProperty("jdbc.port"));
		credentials.put("username", username);
		credentials.put("password", password);
		credentials.put("uri", postgresqlAdminService.getConnectionString(database, username, password, env.getRequiredProperty("jdbc.host"),env.getRequiredProperty("jdbc.port")));
		
		return new ServiceInstanceBinding(request.getBindingId(), instance.getServiceInstanceId(), credentials, null, request.getAppGuid());
	}
}
