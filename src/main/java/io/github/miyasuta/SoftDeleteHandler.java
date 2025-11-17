package io.github.miyasuta;

import com.sap.cds.services.cds.ApplicationService;
import com.sap.cds.services.handler.EventHandler;
import com.sap.cds.services.handler.annotations.ServiceName;

@ServiceName(value = "*", type = ApplicationService.class)
public class SoftDeleteHandler implements EventHandler{
    
}