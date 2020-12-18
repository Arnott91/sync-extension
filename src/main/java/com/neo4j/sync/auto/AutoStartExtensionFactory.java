package com.neo4j.sync.auto;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.internal.LogService;

@ServiceProvider
public class AutoStartExtensionFactory extends ExtensionFactory<AutoStartExtensionFactory.Dependencies> {

    public AutoStartExtensionFactory() {
        super(ExtensionType.DATABASE, "AutoStartExtensionFactory");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext extensionContext, Dependencies dependencies) {
        dependencies.log().getUserLog(this.getClass()).info("AutoStartExtensionFactory#newInstance");
        return new AutoStart(dependencies);
    }

    public interface Dependencies {
        AvailabilityGuard getAvailabilityGuard();

        LogService log();
    }
}
