#include <iostream>
#include <pcm/cpucounters.h>

using namespace pcm;

extern "C" {

void pcm_on() {
    PCM *m = PCM::getInstance();

    EventSelectRegister def_event_select_reg;
    def_event_select_reg.value = 0;
    def_event_select_reg.fields.usr = 1;
    def_event_select_reg.fields.os = 1;
    def_event_select_reg.fields.enable = 1;

    PCM::ExtendedCustomCoreEventDescription conf;
    conf.fixedCfg = NULL; // default
    conf.nGPCounters = 2;

    try {
        m->setupCustomCoreEventsForNuma(conf);
    }
    catch (UnsupportedProcessorException& ) {
        exit(EXIT_FAILURE);
    }

    EventSelectRegister regs[4];
    conf.gpCounterCfg = regs;
    for (int i = 0; i < 4; ++i)
        regs[i] = def_event_select_reg;

    regs[0].fields.event_select = m->getOCREventNr(0, 0).first; // OFFCORE_RESPONSE 0 event
    regs[0].fields.umask =        m->getOCREventNr(0, 0).second;
    regs[1].fields.event_select = m->getOCREventNr(1, 0).first; // OFFCORE_RESPONSE 1 event
    regs[1].fields.umask =        m->getOCREventNr(1, 0).second;

    m->program(PCM::EXT_CUSTOM_CORE_EVENTS, &conf, false);
}

static thread_local SystemCounterState before, after;

void pcm_start() {
    before = getSystemCounterState();
}

uint64_t pcm_get_nr_remote_pmem_access_packet() {
    after = getSystemCounterState();
    uint64_t total = getAllIncomingQPILinkBytes(before, after) / 64 - getNumberOfCustomEvents(1, before, after);
    //uint64_t total = getNumberOfCustomEvents(1, before, after);
    return total;
}

}
