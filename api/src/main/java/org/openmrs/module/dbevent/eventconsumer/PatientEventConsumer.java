package org.openmrs.module.dbevent.eventconsumer;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hl7.fhir.r4.model.Patient;
import org.openmrs.module.dbevent.DbEvent;
import org.openmrs.module.dbevent.EventConsumer;
import org.openmrs.module.fhir2.api.FhirPatientService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Transactional
@Component("dbevent.patientEventConsumer")
public class PatientEventConsumer implements EventConsumer {
    Log log = LogFactory.getLog(PatientEventConsumer.class);

    @Autowired
    @Qualifier("fhirR4")
    private FhirContext fhirContext;

    @Autowired
    private FhirPatientService patientService;

    @Override
    public void accept(DbEvent dbEvent) {
        if (dbEvent.getTable().equals("person")) {
            IParser parser = fhirContext.newJsonParser();
            if (patientService != null) {
                Patient patient = patientService.get(dbEvent.getUuid());
                //Publish message to a message broker
                log.error("Fetched patient "+ parser.encodeResourceToString(patient));
            } else {
                log.error("FhirPatientService BEAN NOT LOADED");
            }
        }
        log.error("UUID " + dbEvent.getUuid() + "for table " + dbEvent.getTable());
    }
}
