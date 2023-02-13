/*
 * This Source Code Form is subject to the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/. OpenMRS is also distributed under
 * the terms of the Healthcare Disclaimer located at http://openmrs.org/license.
 *
 * Copyright (C) OpenMRS Inc. OpenMRS is a registered trademark and the OpenMRS
 * graphic logo is a trademark of OpenMRS Inc.
 */
package org.openmrs.module.dbevent;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.module.BaseModuleActivator;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

/**
 * This class contains the logic that is run every time this module is either started or shutdown
 */
@Component
public class DbEventModuleActivator extends BaseModuleActivator implements ApplicationContextAware{

	Log log = LogFactory.getLog(DbEventModuleActivator.class);

	private static ApplicationContext applicationContext;
	@Autowired
	private EventConsumer patientEventConsumer;

	@Override
	public void started() {
		log.error("DB Event Module Started");
		applicationContext.getAutowireCapableBeanFactory().autowireBean(this);
		EventContext ctx = new EventContext();
		DbEventSourceConfig cfg = new DbEventSourceConfig(1, "TestDBZ", ctx);
		DbEventSource source = new DbEventSource(cfg);
		source.setEventConsumer(patientEventConsumer);
		source.start();
	}

	@Override
	public void stopped() {
		log.error("DB Event Module Stopped");
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		DbEventModuleActivator.applicationContext = applicationContext;
	}
}
