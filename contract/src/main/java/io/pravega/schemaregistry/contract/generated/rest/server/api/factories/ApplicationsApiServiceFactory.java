package io.pravega.schemaregistry.contract.generated.rest.server.api.factories;

import io.pravega.schemaregistry.contract.generated.rest.server.api.ApplicationsApiService;
import io.pravega.schemaregistry.contract.generated.rest.server.api.impl.ApplicationsApiServiceImpl;


public class ApplicationsApiServiceFactory {
    private final static ApplicationsApiService service = new ApplicationsApiServiceImpl();

    public static ApplicationsApiService getApplicationsApi() {
        return service;
    }
}
