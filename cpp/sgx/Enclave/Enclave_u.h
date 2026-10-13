#ifndef ENCLAVE_U_H__
#define ENCLAVE_U_H__

#include <stdint.h>
#include <wchar.h>
#include <stddef.h>
#include <string.h>
#include "sgx_edger8r.h" /* for sgx_status_t etc. */


#include <stdlib.h> /* for size_t */

#define SGX_CAST(type, item) ((type)(item))

#ifdef __cplusplus
extern "C" {
#endif

#ifndef OCALL_PRINT_STRING_DEFINED__
#define OCALL_PRINT_STRING_DEFINED__
void SGX_UBRIDGE(SGX_NOCONVENTION, ocall_print_string, (const char* str));
#endif
#ifndef SGX_OC_CPUIDEX_DEFINED__
#define SGX_OC_CPUIDEX_DEFINED__
void SGX_UBRIDGE(SGX_CDECL, sgx_oc_cpuidex, (int cpuinfo[4], int leaf, int subleaf));
#endif
#ifndef SGX_THREAD_WAIT_UNTRUSTED_EVENT_OCALL_DEFINED__
#define SGX_THREAD_WAIT_UNTRUSTED_EVENT_OCALL_DEFINED__
int SGX_UBRIDGE(SGX_CDECL, sgx_thread_wait_untrusted_event_ocall, (const void* self));
#endif
#ifndef SGX_THREAD_SET_UNTRUSTED_EVENT_OCALL_DEFINED__
#define SGX_THREAD_SET_UNTRUSTED_EVENT_OCALL_DEFINED__
int SGX_UBRIDGE(SGX_CDECL, sgx_thread_set_untrusted_event_ocall, (const void* waiter));
#endif
#ifndef SGX_THREAD_SETWAIT_UNTRUSTED_EVENTS_OCALL_DEFINED__
#define SGX_THREAD_SETWAIT_UNTRUSTED_EVENTS_OCALL_DEFINED__
int SGX_UBRIDGE(SGX_CDECL, sgx_thread_setwait_untrusted_events_ocall, (const void* waiter, const void* self));
#endif
#ifndef SGX_THREAD_SET_MULTIPLE_UNTRUSTED_EVENTS_OCALL_DEFINED__
#define SGX_THREAD_SET_MULTIPLE_UNTRUSTED_EVENTS_OCALL_DEFINED__
int SGX_UBRIDGE(SGX_CDECL, sgx_thread_set_multiple_untrusted_events_ocall, (const void** waiters, size_t total));
#endif

sgx_status_t ecall_Enqueue(sgx_enclave_id_t eid, int* retval, int silo_id, int silo_beg_idx, int num, const uint8_t* aes_key, const uint8_t* aes_iv, const unsigned char* encrypt_data, size_t data_size);
sgx_status_t ecall_OblivQueueHead(sgx_enclave_id_t eid, int* idx, int* silo_id);
sgx_status_t ecall_OblivFreeQueue(sgx_enclave_id_t eid);
sgx_status_t ecall_GetSiloIndex(sgx_enclave_id_t eid, int* retval, int silo_id);
sgx_status_t ecall_GetDataIndex(sgx_enclave_id_t eid, int* retval, int data_id);
sgx_status_t ecall_OblivInitQueue(sgx_enclave_id_t eid, int queue_size);
sgx_status_t ecall_OblivCreateQueue(sgx_enclave_id_t eid, int queue_size);
sgx_status_t ecall_OblivDequeue(sgx_enclave_id_t eid);
sgx_status_t ecall_OblivDequeueMore(sgx_enclave_id_t eid, int num);
sgx_status_t ecall_ClearInformation(sgx_enclave_id_t eid, size_t silo_id);
sgx_status_t ecall_ImportInformation(sgx_enclave_id_t eid, int* retval, size_t silo_id, int importType, size_t data_size, size_t query_k, const uint8_t* aes_key, const uint8_t* aes_iv, const unsigned char* encrypt_data);
sgx_status_t ecall_JointEstimation(sgx_enclave_id_t eid, size_t silo_num, size_t k);
sgx_status_t ecall_GetPrunedK(sgx_enclave_id_t eid, int* retval, size_t silo_id, size_t max_output_size, const uint8_t* aes_key, const uint8_t* aes_iv, unsigned char* encrypt_k);
sgx_status_t ecall_CandRefinement(sgx_enclave_id_t eid, size_t silo_num, size_t k);
sgx_status_t ecall_CandRefinementBase(sgx_enclave_id_t eid, size_t silo_num, size_t k);
sgx_status_t ecall_GetThres(sgx_enclave_id_t eid, int* retval, size_t silo_id, size_t max_output_size, const uint8_t* aes_key, const uint8_t* aes_iv, unsigned char* encrypt_thres);
sgx_status_t ecall_TopkSelection(sgx_enclave_id_t eid, size_t silo_num, size_t k);
sgx_status_t ecall_GetK(sgx_enclave_id_t eid, int* retval, size_t silo_id);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif
