package org.openpaas.servicebroker.postgresql.config;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.openpaas.servicebroker.model.Catalog;
import org.openpaas.servicebroker.model.Plan;
import org.openpaas.servicebroker.model.ServiceDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CatalogConfig {
	
	@Bean
	public Catalog catalog() {		
		return new Catalog( Arrays.asList(
				new ServiceDefinition(
						"postgresql", 
						"PostgreSQL_dDB", 
						"A simple postgresql implementation", 
						true, 
						false,
						getPlans(),
						Arrays.asList("postgresql", "document"),
						getServiceDefinitionMetadata(),
						Arrays.asList("syslog_drain"),
						null)));
	}

	private Map<String,Object> getServiceDefinitionMetadata() {
		Map<String,Object> sdMetadata = new HashMap<String,Object>();
		sdMetadata.put("displayName", "PostgreSQL");
		sdMetadata.put("imageUrl","http://www.openpaas.org/rs/postgresql/images/PostgreSQL_Logo_Full.png");
		sdMetadata.put("longDescription","PostgreSQL Service");
		sdMetadata.put("providerDisplayName","OpenPaaS");
		sdMetadata.put("documentationUrl","http://www.openpaas.org");
		sdMetadata.put("supportUrl","http://www.openpaas.org");
		return sdMetadata;
	}

	private Map<String,Object> getPlanMetadata(String vol, String conncnt) {		
		Map<String,Object> planMetadata = new HashMap<String,Object>();
		planMetadata.put("costs", getCosts());
		planMetadata.put("bullets", getBullets(vol, conncnt));
		return planMetadata;
	}

	private List<Map<String,Object>> getCosts() {
		Map<String,Object> costsMap = new HashMap<String,Object>();

		Map<String,Object> amount = new HashMap<String,Object>();
		amount.put("usd", new Double(0.0));

		costsMap.put("amount", amount);
		costsMap.put("unit", "MONTHLY");

		return Arrays.asList(costsMap);
	}

	private List<String> getBullets(String vol, String conncnt) {
		return Arrays.asList("Shared PostgreSQL server", 
				vol+" Storage", 
				conncnt+" Max Connetions");
	}

	private List<Plan> getPlans() {
		
		List<Plan> plans = Arrays.asList(
				new Plan("postgresql_5", 
						"postgresql_5", 
						"This is a PostgreSQL plan. 100 MB Database volume size and 5 Max Connections set.",
						getPlanMetadata("100 MB", "5"),
						true),
				new Plan("postgresql_10", 
						"postgresql_10", 
						"This is a PostgreSQL plan. 100 MB Database volume size and 10 Max Connections set.",
						getPlanMetadata("100 MB", "10"),
						true));
		return plans;
	}
	
}