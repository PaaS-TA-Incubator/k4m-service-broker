package org.openpaas.servicebroker.model.fixture;

import java.util.ArrayList;
import java.util.List;

import org.openpaas.servicebroker.model.Plan;


public class PlanFixture {

	public static List<Plan> getAllPlans() {
		List<Plan> plans = new ArrayList<Plan>();
		plans.add(getPlanOne());
		plans.add(getPlanTwo());
		return plans;
	}
		
	public static Plan getPlanOne() {
		return new Plan("postgresql_5", "postgresql_5", "Description for Plan One");
	}
	
	public static Plan getPlanTwo() {
		return new Plan("postgresql_10", "postgresql_10", "Description for Plan Two");
	}
	
}
