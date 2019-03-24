package org.apache.nifi.processor

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.components.Validator
import org.apache.nifi.lookup.LookupService
import org.apache.nifi.processor.exception.ProcessException

class MockProcessor extends AbstractProcessor {
    static final PropertyDescriptor LOOKUP_SERVICE = new PropertyDescriptor.Builder()
        .name("lookup")
        .required(false)
        .identifiesControllerService(LookupService.class)
        .addValidator(Validator.VALID)
        .build()

    static final List<PropertyDescriptor> DESCRIPTORS = [ LOOKUP_SERVICE ]
    @Override
    List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        DESCRIPTORS
    }

    @Override
    void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {

    }
}
