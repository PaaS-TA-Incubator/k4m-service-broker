package org.openpaas.servicebroker.postgresql.service.impl;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.openpaas.servicebroker.exception.ServiceBrokerException;
import org.openpaas.servicebroker.model.CreateServiceInstanceBindingRequest;
import org.openpaas.servicebroker.model.CreateServiceInstanceRequest;
import org.openpaas.servicebroker.model.ServiceInstance;
import org.openpaas.servicebroker.model.ServiceInstanceBinding;
import org.openpaas.servicebroker.postgresql.exception.PostgresqlServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

@Service
public class PostgresqlAdminService {

	// Plan별 MAX_USER_CONNECTIONS 정보
	public static String planA = "postgresql_5";
	public static int planAconnections = 5;
	public static String planB = "postgresql_10";
	public static int planBconnections = 10;
	
	private Logger logger = LoggerFactory.getLogger(PostgresqlAdminService.class);
	
	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@Autowired
	public PostgresqlAdminService(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}
		
	private final RowMapper<ServiceInstance> mapper = new ServiceInstanceRowMapper();
	
	private final RowMapper<ServiceInstanceBinding> mapper2 = new ServiceInstanceBindingRowMapper();
	
	/**
	 * ServiceInstance의 유무를 확인합니다
	 * @param instance
	 * @return
	 */
	public boolean isExistsService(ServiceInstance instance){
		System.out.println("PostgresqlAdminService.isExistsService");
		List<String> databases = jdbcTemplate.queryForList("select * from pg_database where datname='"+instance.getServiceInstanceId()+"'",String.class);
		return databases.size() > 0;
	}
	
	/**
	 * 사용자 유무를 확인합니다.
	 * @param userId
	 * @return
	 */
	public boolean isExistsUser(String userId){
		System.out.println("PostgresqlAdminService.isExistsUser");
		
		List<String> databases = jdbcTemplate.queryForList("select * from pg_roles where rolname='"+userId+"'",String.class);
		if(databases.size() > 0) {
			return true;
		}else {
			return false;
		}
	}
	
	/**
	 * ServiceInstanceId로 ServiceInstance정보를 조회합니다.
	 * @param id
	 * @return
	 */
	public ServiceInstance findById(String id){
		System.out.println("PostgresqlAdminService.findById");
		ServiceInstance serviceInstance = null;;
		try {
			serviceInstance = jdbcTemplate.queryForObject("select instance_id as service_instance_id, service_id, plan_id, organization_guid, space_guid from paasta_ins where instance_id = ?", mapper, id);
			serviceInstance.withDashboardUrl(getDashboardUrl(serviceInstance.getServiceInstanceId()));
		} catch (Exception e) {
		}
		return serviceInstance;
	}
	
	/**
	 * 요청 정보로 부터 ServiceInstance를 생성합니다.
	 * @param request
	 * @return
	 */
	public ServiceInstance createServiceInstanceByRequest(CreateServiceInstanceRequest request){
		System.out.println("PostgresqlAdminService.createServiceInstanceByRequest");
		return new ServiceInstance(request).withDashboardUrl(getDashboardUrl(request.getServiceInstanceId()));
	}
	
	/**
	 * ServiceInstanceBindingId로 ServiceInstanceBinding정보를 조회합니다.
	 * @param id
	 * @return
	 */
	public ServiceInstanceBinding findBindById(String id){
		System.out.println("PostgresqlAdminService.findBindById");
		ServiceInstanceBinding serviceInstanceBinding = null;;
		try {
			serviceInstanceBinding = jdbcTemplate.queryForObject("select binding_id, instance_id, app_id, username, password from service_binding where binding_id = ?", mapper2, id);
		} catch (Exception e) {
		}
		return serviceInstanceBinding;
	}
	
	
	/**
	 * ServiceInstanceId로 ServiceInstanceBinding 목록 정보를 조회합니다.
	 * @param id
	 * @return
	 */
	public List<Map<String,Object>> findBindByInstanceId(String id){
		System.out.println("PostgresqlAdminService.findBindByInstanceId");
		//List<ServiceInstanceBinding> list = new ArrayList<ServiceInstanceBinding>();
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
		try {
			list = jdbcTemplate.queryForList("select binding_id, instance_id, app_id, username, password from service_binding where instance_id = ?", id);
		} catch (Exception e) {
		}
		return list;
	}
	
	/**
	 * 요청 정보로부터 ServiceInstanceBinding 정보를 생성합니다.
	 * @param request
	 * @return
	 */
	public ServiceInstanceBinding createServiceInstanceBindingByRequest(CreateServiceInstanceBindingRequest request){
		System.out.println("PostgresqlAdminService.createServiceInstanceBindingByRequest");
		return new ServiceInstanceBinding(request.getBindingId(), 
				request.getServiceInstanceId(), 
				new HashMap<String, Object>(), 
				"syslogDrainUrl", 
				request.getAppGuid());
	}
	
	/**
	 * ServiceInstance 정보를 저장합니다.
	 * @param serviceInstance
	 * @throws PostgresqlServiceException
	 */
	public void save(ServiceInstance serviceInstance) throws PostgresqlServiceException{
		try{
			System.out.println("PostgresqlAdminService.save");
			jdbcTemplate.update("insert into paasta_ins(instance_id, service_id, plan_id, organization_guid, space_guid) values(?,?,?,?,?)", 
					serviceInstance.getServiceInstanceId(),
					serviceInstance.getServiceDefinitionId(),
					serviceInstance.getPlanId(),
					serviceInstance.getOrganizationGuid(),
					serviceInstance.getSpaceGuid()
					//,	serviceInstance.getServiceInstanceId(),
					//serviceInstance.getServiceDefinitionId(),
					//serviceInstance.getPlanId(),
					//serviceInstance.getOrganizationGuid(),
					//serviceInstance.getSpaceGuid()
					);
		} catch (Exception e) {
			throw handleException(e);
		}
	}
	
	/**
	 * ServiceInstanceId로 ServiceInstance정보를 삭제합니다.
	 * @param id
	 * @throws PostgresqlServiceException
	 */
	public void delete(String id) throws PostgresqlServiceException{
		try{
			System.out.println("PostgresqlAdminService.delete");
			jdbcTemplate.update("delete from paasta_ins where instance_id = ?", id);
		} catch (Exception e) {
			throw handleException(e);
		}
	}
	
	/**
	 * 해당하는 Database를 삭제합니다.
	 * @param serviceInstanceBinding
	 * @throws PostgresqlServiceException
	 */
//	public void deleteDatabase(ServiceInstanceBinding serviceInstanceBinding) throws PostgresqlServiceException{
//		try{
//			System.out.println("PostgresqlAdminService.deleteDatabase");
//			jdbcTemplate.execute("DROP DATABASE IF EXISTS " + serviceInstanceBinding.getId());
//		} catch (Exception e) {
//			throw handleException(e);
//		}
//	}
	
	/**
	 * 해당하는 Database를 생성합니다.
	 * @param serviceInstance
	 * @throws PostgresqlServiceException
	 */
//	public void createDatabase(ServiceInstanceBinding serviceInstanceBinding) throws PostgresqlServiceException{
//		
//		try{
//			
//			System.out.println("PostgresqlAdminService.createDatabase");
//			jdbcTemplate.execute("CREATE DATABASE " + serviceInstanceBinding.getId());
//		} catch (Exception e) {
//			throw handleException(e);
//		}
//	}
	
	/**
	 * ServiceInstance의 Plan 정보를 수정합니다.
	 * @param instance
	 * @param request
	 * @throws PostgresqlServiceException
	 */
	public void updatePlan(ServiceInstance updatedInstance) throws PostgresqlServiceException{
		try{
			System.out.println("PostgresqlAdminService.updatePlan");
			/*if(!updatedInstance.getServiceInstanceId().isEmpty() && !updatedInstance.getServiceDefinitionId().isEmpty()) {
				jdbcTemplate.update("UPDATE paasta_ins SET service_id = ? where instance_id = ?",
						updatedInstance.getServiceDefinitionId(),
						updatedInstance.getServiceInstanceId());
			}*/
			jdbcTemplate.update("UPDATE paasta_ins SET service_id = ?, plan_id = ?, organization_guid = ?, space_guid = ? where instance_id = ?",
					updatedInstance.getServiceDefinitionId(),
					updatedInstance.getPlanId(),
					updatedInstance.getOrganizationGuid(),
					updatedInstance.getSpaceGuid(),
					updatedInstance.getServiceInstanceId());
		} catch (Exception e) {
			throw handleException(e);
		}
	}
	
	/**
	 * 사용자별 MAX_USER_CONNECTIONS정보를 수정합니다.
	 * @param database
	 * @param userId
	 * @param connections
	 * @return
	 */
	public boolean updateUserConnections(String database, String userId, String connections){
		System.out.println("PostgresqlAdminService.updateUserConnections");
		try {
			//jdbcTemplate.execute("GRANT USAGE ON " + database + ".* TO '" + userId + "'@'%' WITH MAX_USER_CONNECTIONS " + connections);
			jdbcTemplate.execute("alter role " + userId + " connection limit " + connections);
		} catch (Exception e) {
			return false;
		}
		return true;
	}
	
	/**
	 * ServiceInstanceBinding 정보를 저장합니다.
	 * @param serviceInstanceBinding
	 * @throws PostgresqlServiceException
	 */
	public void saveBind(ServiceInstanceBinding serviceInstanceBinding) throws PostgresqlServiceException{
		try{
			System.out.println("PostgresqlAdminService.saveBind");
			jdbcTemplate.update("insert into service_binding(binding_id, instance_id, app_id, username, password) values(?,?,?,?,?)", 
					serviceInstanceBinding.getId(),
					serviceInstanceBinding.getServiceInstanceId(),
					serviceInstanceBinding.getAppGuid(),
					serviceInstanceBinding.getCredentials().get("username"),
					serviceInstanceBinding.getCredentials().get("password")
					
					//serviceInstanceBinding.getServiceInstanceId(),
					//serviceInstanceBinding.getAppGuid(),
					//serviceInstanceBinding.getCredentials().get("username"),
					//serviceInstanceBinding.getCredentials().get("password")
					);
		} catch (Exception e) {
			throw handleException(e);
		}
	}
	
	/**
	 * ServiceInstanceBindingId로 ServiceInstanceBinding 정보를 삭제합니다.
	 * @param id
	 * @throws PostgresqlServiceException
	 */
	public void deleteBind(String id) throws PostgresqlServiceException{
		try{
			System.out.println("PostgresqlAdminService.deleteBind");
			jdbcTemplate.update("delete from service_binding where binding_id = ?", id);
		} catch (Exception e) {
			throw handleException(e);
		}
	}
	
	/**
	 * 해당하는 User를 생성합니다.
	 * @param database
	 * @param userId
	 * @param password
	 * @throws PostgresqlServiceException
	 */
	public void createUser(String database, String userId, String password) throws PostgresqlServiceException{
		try{
			System.out.println("PostgresqlAdminService.createUser");
			jdbcTemplate.execute("CREATE ROLE "+userId+" WITH LOGIN PASSWORD '"+password+"'");
			jdbcTemplate.execute("CREATE DATABASE " +database+ " OWNER="+userId);
			jdbcTemplate.execute("REVOKE all on database " +database+ " from public");
			//jdbcTemplate.execute("\\"+"c "+database);
			//jdbcTemplate.execute("CREATE SCHEMA IF NOT EXISTS "+userId+" AUTHORIZATION "+userId);
			//jdbcTemplate.execute("ALTER ROLE "+userId+" SET search_path = "+userId+",etc");
		} catch (Exception e) {
			throw handleException(e);
		}
	}
	
	//public void deleteUser(String database, String userId) throws MysqlServiceException{
	/**
	 * 해당하는 User를 삭제합니다.
	 * @param database
	 * @param bindingId
	 * @throws PostgresqlServiceException
	 */
	public void deleteUser( String bindingId) throws PostgresqlServiceException{
		try{
			System.out.println("PostgresqlAdminService.deleteUser");
			String userId = jdbcTemplate.queryForObject("select username from service_binding where binding_id = ?", String.class, bindingId);
			//jdbcTemplate.execute("\\"+"c "+database);
			jdbcTemplate.execute("DROP DATABASE IF EXISTS " + userId);
			jdbcTemplate.execute("DROP ROLE "+userId);
			//jdbcTemplate.execute("DROP SCHEMA IF EXISTS "+userId);
		} catch (Exception e) {
			//throw handleException(e);
		}
	}
	
	/**
	 * 해당하는 User를 삭제합니다.
	 * @param database
	 * @param bindingId
	 * @throws PostgresqlServiceException
	 */
//	public void deleteUser(String userId) throws PostgresqlServiceException{
//		try{
//			System.out.println("PostgresqlAdminService.deleteUser");
//			jdbcTemplate.execute("DROP ROLE "+userId);
//		} catch (Exception e) {
//			throw handleException(e);
//		}
//	}
	
	/*public String getConnectionString(String database, String username, String password) {
		//mysql://b5d435f40dd2b2:ebfc00ac@us-cdbr-east-03.cleardb.com:3306/ad_c6f4446532610ab
		StringBuilder builder = new StringBuilder();
		return builder.toString();
	}*/
	
	/**
	 * Database 접속정보를 생성합니다.
	 * @param database
	 * @param username
	 * @param password
	 * @param hostName
	 * @return
	 */
	public String getConnectionString(String database, String username, String password, String hostName, String port) {
		StringBuilder builder = new StringBuilder();
		builder.append("postgresql://"+username+":"+password+"@"+hostName+":"+port+"/"+database);
		return builder.toString();
	}
	
	public String getServerAddresses() {
		StringBuilder builder = new StringBuilder();
		return builder.toString();
	}
	
	private PostgresqlServiceException handleException(Exception e) {
		logger.warn(e.getLocalizedMessage(), e);
		return new PostgresqlServiceException(e.getLocalizedMessage());
	}
	
	// DashboardUrl 생성
	public String getDashboardUrl(String instanceId){
		
		return "http://www.sample.com/"+instanceId;
	}
	
	// User명 생성
	public String getUsername(String id) {
		MessageDigest digest = null;
		try {
			digest = MessageDigest.getInstance("MD5");
		} catch (Exception e) {
			// TODO: handle exception
		}
		digest.update(id.getBytes());
		String username = "ex_" + new BigInteger(1, digest.digest()).toString(16).replaceAll("/[^a-zA-Z0-9]+/", "").substring(0, 16);
		return username;
	}
	
	// User MAX_USER_CONNECTIONS 설정 조정
	public void setUserConnections(String planId, String id) throws ServiceBrokerException{
		
		/* Plan 정보 설정 */
		int totalConnections = 0;
		int totalUsers;
		int userPerConnections;
		int mod;
		
		if(planA.equals(planId)) totalConnections = planAconnections;
		if(planB.equals(planId)) totalConnections = planBconnections;
		if(!planA.equals(planId) && !planB.equals(planId)) throw new ServiceBrokerException("");
		
		// ServiceInstanceBinding 정보를 조회한다.
		List<Map<String,Object>> list = findBindByInstanceId(id);
		// ServiceInstance의 총 Binding 건수 확인
		totalUsers = list.size();
		if(totalUsers <= 0) return;
		if(totalConnections<totalUsers) throw new ServiceBrokerException("It may not exceed the specified plan.(Not assign Max User Connection)");

		// User당 connection 수 = Plan의 connection / 총 Binding 건수
		userPerConnections = totalConnections / totalUsers;
		// 미할당 connection = Plan의 connection % 총 Binding 건수
		mod = totalConnections % totalUsers;

		for(int i=0;i < list.size();i++){
			Map<String,Object> tmp = list.get(i);
			
			// 첫번째 사용자에게 User당 connection 수 + 미할당 connection 할당
			if(i==0){
				updateUserConnections((String)tmp.get("instance_id"), getUsername((String)tmp.get("binding_id")), (userPerConnections+mod)+"");
				//System.out.println(tmp.get("instance_id")+"/"+tmp.get("binding_id")+"/"+userPerConnections);
			}
			//  User당 connection 수  할당
			else{
				updateUserConnections((String)tmp.get("instance_id"), getUsername((String)tmp.get("binding_id")), userPerConnections+"");
				//System.out.println(tmp.get("instance_id")+"/"+tmp.get("binding_id")+"/"+userPerConnections);
			}
		}
	}
	
	// User MAX_USER_CONNECTIONS 설정 조정
	public boolean checkUserConnections(String planId, String id) throws ServiceBrokerException{
			
		/* Plan 정보 설정 */
		int totalConnections = 0;
		int totalUsers;
		
		if(planA.equals(planId)) totalConnections = planAconnections;
		if(planB.equals(planId)) totalConnections = planBconnections;
		if(!planA.equals(planId) && !planB.equals(planId)) throw new ServiceBrokerException("");
		
		// ServiceInstanceBinding 정보를 조회한다.
		List<Map<String,Object>> list = findBindByInstanceId(id);
		// ServiceInstance의 총 Binding 건수 확인
		totalUsers = list.size();
		
		if(totalConnections <= totalUsers) return true;
		
		return false;
	}
	
	private static final class ServiceInstanceRowMapper implements RowMapper<ServiceInstance> {
        @Override
        public ServiceInstance mapRow(ResultSet rs, int rowNum) throws SQLException {
            CreateServiceInstanceRequest request = new CreateServiceInstanceRequest();
            request.withServiceInstanceId(rs.getString(1));
            request.setServiceDefinitionId(rs.getString(2));
            request.setPlanId(rs.getString(3));
            request.setOrganizationGuid(rs.getString(4));
            request.setSpaceGuid(rs.getString(5));
            return new ServiceInstance(request);
        }
    }
	
	private static final class ServiceInstanceBindingRowMapper implements RowMapper<ServiceInstanceBinding> {
        @Override
        public ServiceInstanceBinding mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new ServiceInstanceBinding(rs.getString(1), 
            		rs.getString(2), 
            		new HashMap<String, Object>(), 
            		"", 
            		rs.getString(3));
        }
    }
	
	
}
