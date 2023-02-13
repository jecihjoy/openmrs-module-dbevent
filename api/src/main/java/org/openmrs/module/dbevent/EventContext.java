package org.openmrs.module.dbevent;

import lombok.Data;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.api.context.Context;

import java.io.File;
import java.util.Properties;

/**
 * Simple wrapper class access that provides access to the OpenMRS Context and related services
 */
@Data
public class EventContext {

    private static final Log log = LogFactory.getLog(EventContext.class);

    private File applicationDataDir;
    private Properties runtimeProperties;

    public EventContext() {
        applicationDataDir = null; //OpenmrsUtil.getApplicationDataDirectoryAsFile();
        runtimeProperties = Context.getRuntimeProperties();
    }

    /**
     * @return a database object constructed from the given runtime properties.
     */
    public Database getDatabase() {
        return new Database(runtimeProperties);
    }

    /**
     * @return the directory for module-related data
     */
    public File getModuleDataDir() {
        File file = new File("/var/lib/OpenMRS");
        return new File(file, "dbevent");
    }
}
