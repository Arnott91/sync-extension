package com.neo4j.sync.start;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.internal.LogService;

@ServiceProvider
public class StartupExtensionFactory extends ExtensionFactory<StartupExtensionFactory.Dependencies> {

    public StartupExtensionFactory() {
        super(ExtensionType.DATABASE, "StartupExtensionFactory");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext extensionContext, Dependencies dependencies) {
        dependencies.log().getUserLog(this.getClass()).info("StartupExtensionFactory#newInstance");
        return new Startup(dependencies);
    }

    public interface Dependencies {
        AvailabilityGuard getAvailabilityGuard();
        DatabaseManagementService getDatabaseManagementService();
        LogService log();
    }
}
