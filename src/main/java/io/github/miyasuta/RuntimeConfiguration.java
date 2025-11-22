package io.github.miyasuta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sap.cds.services.runtime.CdsRuntimeConfiguration;
import com.sap.cds.services.runtime.CdsRuntimeConfigurer;

public class RuntimeConfiguration implements CdsRuntimeConfiguration{
    private static final Logger logger = LoggerFactory.getLogger(RuntimeConfiguration.class);

    @Override
    public void eventHandlers(CdsRuntimeConfigurer configurer) {
        configurer.eventHandler(new SoftDeleteHandler());
        logger.info("[cds-feature-softdelete] SoftDeleteHandler successfully registered 5:29 ✅");
    }
}
