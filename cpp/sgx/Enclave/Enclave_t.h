#ifndef ENCLAVE_T_H__
#define ENCLAVE_T_H__

#include <stdint.h>
#include <wchar.h>
#include <stddef.h>
#include "sgx_edger8r.h" /* for sgx_ocall etc. */


#include <stdlib.h> /* for size_t */

#define SGX_CAST(type, item) ((type)(item))

#ifdef __cplusplus
extern "C" {
#endif

int ecall_Enqueue(int silo_id, int silo_beg_idx, int num, const uint8_t* aes_key, const uint8_t* aes_iv, const unsigned char* encrypt_data, size_t data_size);
void ecall_OblivQueueHead(int* idx, int* silo_id);
void ecall_OblivFreeQueue(void);
int ecall_GetSiloIndex(int silo_id);
int ecall_GetDataIndex(int data_id);
void ecall_OblivInitQueue(int queue_size);
void ecall_OblivCreateQueue(int queue_size);
void ecall_OblivDequeue(void);
void ecall_OblivDequeueMore(int num);
void ecall_ClearInformation(size_t silo_id);
int ecall_ImportInformation(size_t silo_id, int importType, size_t data_size, size_t query_k, const uint8_t* aes_key, const uint8_t* aes_iv, const unsigned char* encrypt_data);
void ecall_JointEstimation(size_t silo_num, size_t k);
int ecall_GetPrunedK(size_t silo_id, size_t max_output_size, const uint8_t* aes_key, const uint8_t* aes_iv, unsigned char* encrypt_k);
void ecall_CandRefinement(size_t silo_num, size_t k);
void ecall_CandRefinementBase(size_t silo_num, size_t k);
int ecall_GetThres(size_t silo_id, size_t max_output_size, const uint8_t* aes_key, const uint8_t* aes_iv, unsigned char* encrypt_thres);
void ecall_TopkSelection(size_t silo_num, size_t k);
int ecall_GetK(size_t silo_id);

sgx_status_t SGX_CDECL ocall_print_string(const char* str);
sgx_status_t SGX_CDECL sgx_oc_cpuidex(int cpuinfo[4], int leaf, int subleaf);
sgx_status_t SGX_CDECL sgx_thread_wait_untrusted_event_ocall(int* retval, const void* self);
sgx_status_t SGX_CDECL sgx_thread_set_untrusted_event_ocall(int* retval, const void* waiter);
sgx_status_t SGX_CDECL sgx_thread_setwait_untrusted_events_ocall(int* retval, const void* waiter, const void* self);
sgx_status_t SGX_CDECL sgx_thread_set_multiple_untrusted_events_ocall(int* retval, const void** waiters, size_t total);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif
