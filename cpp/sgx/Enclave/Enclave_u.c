#include "Enclave_u.h"
#include <errno.h>

typedef struct ms_ecall_Enqueue_t {
	int ms_retval;
	int ms_silo_id;
	int ms_silo_beg_idx;
	int ms_num;
	const uint8_t* ms_aes_key;
	const uint8_t* ms_aes_iv;
	const unsigned char* ms_encrypt_data;
	size_t ms_data_size;
} ms_ecall_Enqueue_t;

typedef struct ms_ecall_OblivQueueHead_t {
	int* ms_idx;
	int* ms_silo_id;
} ms_ecall_OblivQueueHead_t;

typedef struct ms_ecall_GetSiloIndex_t {
	int ms_retval;
	int ms_silo_id;
} ms_ecall_GetSiloIndex_t;

typedef struct ms_ecall_GetDataIndex_t {
	int ms_retval;
	int ms_data_id;
} ms_ecall_GetDataIndex_t;

typedef struct ms_ecall_OblivInitQueue_t {
	int ms_queue_size;
} ms_ecall_OblivInitQueue_t;

typedef struct ms_ecall_OblivCreateQueue_t {
	int ms_queue_size;
} ms_ecall_OblivCreateQueue_t;

typedef struct ms_ecall_OblivDequeueMore_t {
	int ms_num;
} ms_ecall_OblivDequeueMore_t;

typedef struct ms_ecall_ClearInformation_t {
	size_t ms_silo_id;
} ms_ecall_ClearInformation_t;

typedef struct ms_ecall_ImportInformation_t {
	int ms_retval;
	size_t ms_silo_id;
	int ms_importType;
	size_t ms_data_size;
	size_t ms_query_k;
	const uint8_t* ms_aes_key;
	const uint8_t* ms_aes_iv;
	const unsigned char* ms_encrypt_data;
} ms_ecall_ImportInformation_t;

typedef struct ms_ecall_JointEstimation_t {
	size_t ms_silo_num;
	size_t ms_k;
} ms_ecall_JointEstimation_t;

typedef struct ms_ecall_GetPrunedK_t {
	int ms_retval;
	size_t ms_silo_id;
	size_t ms_max_output_size;
	const uint8_t* ms_aes_key;
	const uint8_t* ms_aes_iv;
	unsigned char* ms_encrypt_k;
} ms_ecall_GetPrunedK_t;

typedef struct ms_ecall_CandRefinement_t {
	size_t ms_silo_num;
	size_t ms_k;
} ms_ecall_CandRefinement_t;

typedef struct ms_ecall_CandRefinementBase_t {
	size_t ms_silo_num;
	size_t ms_k;
} ms_ecall_CandRefinementBase_t;

typedef struct ms_ecall_GetThres_t {
	int ms_retval;
	size_t ms_silo_id;
	size_t ms_max_output_size;
	const uint8_t* ms_aes_key;
	const uint8_t* ms_aes_iv;
	unsigned char* ms_encrypt_thres;
} ms_ecall_GetThres_t;

typedef struct ms_ecall_TopkSelection_t {
	size_t ms_silo_num;
	size_t ms_k;
} ms_ecall_TopkSelection_t;

typedef struct ms_ecall_GetK_t {
	int ms_retval;
	size_t ms_silo_id;
} ms_ecall_GetK_t;

typedef struct ms_ocall_print_string_t {
	const char* ms_str;
} ms_ocall_print_string_t;

typedef struct ms_sgx_oc_cpuidex_t {
	int* ms_cpuinfo;
	int ms_leaf;
	int ms_subleaf;
} ms_sgx_oc_cpuidex_t;

typedef struct ms_sgx_thread_wait_untrusted_event_ocall_t {
	int ms_retval;
	const void* ms_self;
} ms_sgx_thread_wait_untrusted_event_ocall_t;

typedef struct ms_sgx_thread_set_untrusted_event_ocall_t {
	int ms_retval;
	const void* ms_waiter;
} ms_sgx_thread_set_untrusted_event_ocall_t;

typedef struct ms_sgx_thread_setwait_untrusted_events_ocall_t {
	int ms_retval;
	const void* ms_waiter;
	const void* ms_self;
} ms_sgx_thread_setwait_untrusted_events_ocall_t;

typedef struct ms_sgx_thread_set_multiple_untrusted_events_ocall_t {
	int ms_retval;
	const void** ms_waiters;
	size_t ms_total;
} ms_sgx_thread_set_multiple_untrusted_events_ocall_t;

static sgx_status_t SGX_CDECL Enclave_ocall_print_string(void* pms)
{
	ms_ocall_print_string_t* ms = SGX_CAST(ms_ocall_print_string_t*, pms);
	ocall_print_string(ms->ms_str);

	return SGX_SUCCESS;
}

static sgx_status_t SGX_CDECL Enclave_sgx_oc_cpuidex(void* pms)
{
	ms_sgx_oc_cpuidex_t* ms = SGX_CAST(ms_sgx_oc_cpuidex_t*, pms);
	sgx_oc_cpuidex(ms->ms_cpuinfo, ms->ms_leaf, ms->ms_subleaf);

	return SGX_SUCCESS;
}

static sgx_status_t SGX_CDECL Enclave_sgx_thread_wait_untrusted_event_ocall(void* pms)
{
	ms_sgx_thread_wait_untrusted_event_ocall_t* ms = SGX_CAST(ms_sgx_thread_wait_untrusted_event_ocall_t*, pms);
	ms->ms_retval = sgx_thread_wait_untrusted_event_ocall(ms->ms_self);

	return SGX_SUCCESS;
}

static sgx_status_t SGX_CDECL Enclave_sgx_thread_set_untrusted_event_ocall(void* pms)
{
	ms_sgx_thread_set_untrusted_event_ocall_t* ms = SGX_CAST(ms_sgx_thread_set_untrusted_event_ocall_t*, pms);
	ms->ms_retval = sgx_thread_set_untrusted_event_ocall(ms->ms_waiter);

	return SGX_SUCCESS;
}

static sgx_status_t SGX_CDECL Enclave_sgx_thread_setwait_untrusted_events_ocall(void* pms)
{
	ms_sgx_thread_setwait_untrusted_events_ocall_t* ms = SGX_CAST(ms_sgx_thread_setwait_untrusted_events_ocall_t*, pms);
	ms->ms_retval = sgx_thread_setwait_untrusted_events_ocall(ms->ms_waiter, ms->ms_self);

	return SGX_SUCCESS;
}

static sgx_status_t SGX_CDECL Enclave_sgx_thread_set_multiple_untrusted_events_ocall(void* pms)
{
	ms_sgx_thread_set_multiple_untrusted_events_ocall_t* ms = SGX_CAST(ms_sgx_thread_set_multiple_untrusted_events_ocall_t*, pms);
	ms->ms_retval = sgx_thread_set_multiple_untrusted_events_ocall(ms->ms_waiters, ms->ms_total);

	return SGX_SUCCESS;
}

static const struct {
	size_t nr_ocall;
	void * table[6];
} ocall_table_Enclave = {
	6,
	{
		(void*)Enclave_ocall_print_string,
		(void*)Enclave_sgx_oc_cpuidex,
		(void*)Enclave_sgx_thread_wait_untrusted_event_ocall,
		(void*)Enclave_sgx_thread_set_untrusted_event_ocall,
		(void*)Enclave_sgx_thread_setwait_untrusted_events_ocall,
		(void*)Enclave_sgx_thread_set_multiple_untrusted_events_ocall,
	}
};
sgx_status_t ecall_Enqueue(sgx_enclave_id_t eid, int* retval, int silo_id, int silo_beg_idx, int num, const uint8_t* aes_key, const uint8_t* aes_iv, const unsigned char* encrypt_data, size_t data_size)
{
	sgx_status_t status;
	ms_ecall_Enqueue_t ms;
	ms.ms_silo_id = silo_id;
	ms.ms_silo_beg_idx = silo_beg_idx;
	ms.ms_num = num;
	ms.ms_aes_key = aes_key;
	ms.ms_aes_iv = aes_iv;
	ms.ms_encrypt_data = encrypt_data;
	ms.ms_data_size = data_size;
	status = sgx_ecall(eid, 0, &ocall_table_Enclave, &ms);
	if (status == SGX_SUCCESS && retval) *retval = ms.ms_retval;
	return status;
}

sgx_status_t ecall_OblivQueueHead(sgx_enclave_id_t eid, int* idx, int* silo_id)
{
	sgx_status_t status;
	ms_ecall_OblivQueueHead_t ms;
	ms.ms_idx = idx;
	ms.ms_silo_id = silo_id;
	status = sgx_ecall(eid, 1, &ocall_table_Enclave, &ms);
	return status;
}

sgx_status_t ecall_OblivFreeQueue(sgx_enclave_id_t eid)
{
	sgx_status_t status;
	status = sgx_ecall(eid, 2, &ocall_table_Enclave, NULL);
	return status;
}

sgx_status_t ecall_GetSiloIndex(sgx_enclave_id_t eid, int* retval, int silo_id)
{
	sgx_status_t status;
	ms_ecall_GetSiloIndex_t ms;
	ms.ms_silo_id = silo_id;
	status = sgx_ecall(eid, 3, &ocall_table_Enclave, &ms);
	if (status == SGX_SUCCESS && retval) *retval = ms.ms_retval;
	return status;
}

sgx_status_t ecall_GetDataIndex(sgx_enclave_id_t eid, int* retval, int data_id)
{
	sgx_status_t status;
	ms_ecall_GetDataIndex_t ms;
	ms.ms_data_id = data_id;
	status = sgx_ecall(eid, 4, &ocall_table_Enclave, &ms);
	if (status == SGX_SUCCESS && retval) *retval = ms.ms_retval;
	return status;
}

sgx_status_t ecall_OblivInitQueue(sgx_enclave_id_t eid, int queue_size)
{
	sgx_status_t status;
	ms_ecall_OblivInitQueue_t ms;
	ms.ms_queue_size = queue_size;
	status = sgx_ecall(eid, 5, &ocall_table_Enclave, &ms);
	return status;
}

sgx_status_t ecall_OblivCreateQueue(sgx_enclave_id_t eid, int queue_size)
{
	sgx_status_t status;
	ms_ecall_OblivCreateQueue_t ms;
	ms.ms_queue_size = queue_size;
	status = sgx_ecall(eid, 6, &ocall_table_Enclave, &ms);
	return status;
}

sgx_status_t ecall_OblivDequeue(sgx_enclave_id_t eid)
{
	sgx_status_t status;
	status = sgx_ecall(eid, 7, &ocall_table_Enclave, NULL);
	return status;
}

sgx_status_t ecall_OblivDequeueMore(sgx_enclave_id_t eid, int num)
{
	sgx_status_t status;
	ms_ecall_OblivDequeueMore_t ms;
	ms.ms_num = num;
	status = sgx_ecall(eid, 8, &ocall_table_Enclave, &ms);
	return status;
}

sgx_status_t ecall_ClearInformation(sgx_enclave_id_t eid, size_t silo_id)
{
	sgx_status_t status;
	ms_ecall_ClearInformation_t ms;
	ms.ms_silo_id = silo_id;
	status = sgx_ecall(eid, 9, &ocall_table_Enclave, &ms);
	return status;
}

sgx_status_t ecall_ImportInformation(sgx_enclave_id_t eid, int* retval, size_t silo_id, int importType, size_t data_size, size_t query_k, const uint8_t* aes_key, const uint8_t* aes_iv, const unsigned char* encrypt_data)
{
	sgx_status_t status;
	ms_ecall_ImportInformation_t ms;
	ms.ms_silo_id = silo_id;
	ms.ms_importType = importType;
	ms.ms_data_size = data_size;
	ms.ms_query_k = query_k;
	ms.ms_aes_key = aes_key;
	ms.ms_aes_iv = aes_iv;
	ms.ms_encrypt_data = encrypt_data;
	status = sgx_ecall(eid, 10, &ocall_table_Enclave, &ms);
	if (status == SGX_SUCCESS && retval) *retval = ms.ms_retval;
	return status;
}

sgx_status_t ecall_JointEstimation(sgx_enclave_id_t eid, size_t silo_num, size_t k)
{
	sgx_status_t status;
	ms_ecall_JointEstimation_t ms;
	ms.ms_silo_num = silo_num;
	ms.ms_k = k;
	status = sgx_ecall(eid, 11, &ocall_table_Enclave, &ms);
	return status;
}

sgx_status_t ecall_GetPrunedK(sgx_enclave_id_t eid, int* retval, size_t silo_id, size_t max_output_size, const uint8_t* aes_key, const uint8_t* aes_iv, unsigned char* encrypt_k)
{
	sgx_status_t status;
	ms_ecall_GetPrunedK_t ms;
	ms.ms_silo_id = silo_id;
	ms.ms_max_output_size = max_output_size;
	ms.ms_aes_key = aes_key;
	ms.ms_aes_iv = aes_iv;
	ms.ms_encrypt_k = encrypt_k;
	status = sgx_ecall(eid, 12, &ocall_table_Enclave, &ms);
	if (status == SGX_SUCCESS && retval) *retval = ms.ms_retval;
	return status;
}

sgx_status_t ecall_CandRefinement(sgx_enclave_id_t eid, size_t silo_num, size_t k)
{
	sgx_status_t status;
	ms_ecall_CandRefinement_t ms;
	ms.ms_silo_num = silo_num;
	ms.ms_k = k;
	status = sgx_ecall(eid, 13, &ocall_table_Enclave, &ms);
	return status;
}

sgx_status_t ecall_CandRefinementBase(sgx_enclave_id_t eid, size_t silo_num, size_t k)
{
	sgx_status_t status;
	ms_ecall_CandRefinementBase_t ms;
	ms.ms_silo_num = silo_num;
	ms.ms_k = k;
	status = sgx_ecall(eid, 14, &ocall_table_Enclave, &ms);
	return status;
}

sgx_status_t ecall_GetThres(sgx_enclave_id_t eid, int* retval, size_t silo_id, size_t max_output_size, const uint8_t* aes_key, const uint8_t* aes_iv, unsigned char* encrypt_thres)
{
	sgx_status_t status;
	ms_ecall_GetThres_t ms;
	ms.ms_silo_id = silo_id;
	ms.ms_max_output_size = max_output_size;
	ms.ms_aes_key = aes_key;
	ms.ms_aes_iv = aes_iv;
	ms.ms_encrypt_thres = encrypt_thres;
	status = sgx_ecall(eid, 15, &ocall_table_Enclave, &ms);
	if (status == SGX_SUCCESS && retval) *retval = ms.ms_retval;
	return status;
}

sgx_status_t ecall_TopkSelection(sgx_enclave_id_t eid, size_t silo_num, size_t k)
{
	sgx_status_t status;
	ms_ecall_TopkSelection_t ms;
	ms.ms_silo_num = silo_num;
	ms.ms_k = k;
	status = sgx_ecall(eid, 16, &ocall_table_Enclave, &ms);
	return status;
}

sgx_status_t ecall_GetK(sgx_enclave_id_t eid, int* retval, size_t silo_id)
{
	sgx_status_t status;
	ms_ecall_GetK_t ms;
	ms.ms_silo_id = silo_id;
	status = sgx_ecall(eid, 17, &ocall_table_Enclave, &ms);
	if (status == SGX_SUCCESS && retval) *retval = ms.ms_retval;
	return status;
}

