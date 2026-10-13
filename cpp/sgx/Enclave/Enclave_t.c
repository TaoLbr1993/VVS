#include "Enclave_t.h"

#include "sgx_trts.h" /* for sgx_ocalloc, sgx_is_outside_enclave */
#include "sgx_lfence.h" /* for sgx_lfence */

#include <errno.h>
#include <mbusafecrt.h> /* for memcpy_s etc */
#include <stdlib.h> /* for malloc/free etc */

#define CHECK_REF_POINTER(ptr, siz) do {	\
	if (!(ptr) || ! sgx_is_outside_enclave((ptr), (siz)))	\
		return SGX_ERROR_INVALID_PARAMETER;\
} while (0)

#define CHECK_UNIQUE_POINTER(ptr, siz) do {	\
	if ((ptr) && ! sgx_is_outside_enclave((ptr), (siz)))	\
		return SGX_ERROR_INVALID_PARAMETER;\
} while (0)

#define CHECK_ENCLAVE_POINTER(ptr, siz) do {	\
	if ((ptr) && ! sgx_is_within_enclave((ptr), (siz)))	\
		return SGX_ERROR_INVALID_PARAMETER;\
} while (0)

#define ADD_ASSIGN_OVERFLOW(a, b) (	\
	((a) += (b)) < (b)	\
)


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

static sgx_status_t SGX_CDECL sgx_ecall_Enqueue(void* pms)
{
	CHECK_REF_POINTER(pms, sizeof(ms_ecall_Enqueue_t));
	//
	// fence after pointer checks
	//
	sgx_lfence();
	ms_ecall_Enqueue_t* ms = SGX_CAST(ms_ecall_Enqueue_t*, pms);
	ms_ecall_Enqueue_t __in_ms;
	if (memcpy_s(&__in_ms, sizeof(ms_ecall_Enqueue_t), ms, sizeof(ms_ecall_Enqueue_t))) {
		return SGX_ERROR_UNEXPECTED;
	}
	sgx_status_t status = SGX_SUCCESS;
	const uint8_t* _tmp_aes_key = __in_ms.ms_aes_key;
	size_t _len_aes_key = 16 * sizeof(uint8_t);
	uint8_t* _in_aes_key = NULL;
	const uint8_t* _tmp_aes_iv = __in_ms.ms_aes_iv;
	size_t _len_aes_iv = 16 * sizeof(uint8_t);
	uint8_t* _in_aes_iv = NULL;
	const unsigned char* _tmp_encrypt_data = __in_ms.ms_encrypt_data;
	size_t _tmp_data_size = __in_ms.ms_data_size;
	size_t _len_encrypt_data = _tmp_data_size * sizeof(unsigned char);
	unsigned char* _in_encrypt_data = NULL;
	int _in_retval;

	if (sizeof(*_tmp_aes_key) != 0 &&
		16 > (SIZE_MAX / sizeof(*_tmp_aes_key))) {
		return SGX_ERROR_INVALID_PARAMETER;
	}

	if (sizeof(*_tmp_aes_iv) != 0 &&
		16 > (SIZE_MAX / sizeof(*_tmp_aes_iv))) {
		return SGX_ERROR_INVALID_PARAMETER;
	}

	if (sizeof(*_tmp_encrypt_data) != 0 &&
		(size_t)_tmp_data_size > (SIZE_MAX / sizeof(*_tmp_encrypt_data))) {
		return SGX_ERROR_INVALID_PARAMETER;
	}

	CHECK_UNIQUE_POINTER(_tmp_aes_key, _len_aes_key);
	CHECK_UNIQUE_POINTER(_tmp_aes_iv, _len_aes_iv);
	CHECK_UNIQUE_POINTER(_tmp_encrypt_data, _len_encrypt_data);

	//
	// fence after pointer checks
	//
	sgx_lfence();

	if (_tmp_aes_key != NULL && _len_aes_key != 0) {
		if ( _len_aes_key % sizeof(*_tmp_aes_key) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		_in_aes_key = (uint8_t*)malloc(_len_aes_key);
		if (_in_aes_key == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		if (memcpy_s(_in_aes_key, _len_aes_key, _tmp_aes_key, _len_aes_key)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}

	}
	if (_tmp_aes_iv != NULL && _len_aes_iv != 0) {
		if ( _len_aes_iv % sizeof(*_tmp_aes_iv) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		_in_aes_iv = (uint8_t*)malloc(_len_aes_iv);
		if (_in_aes_iv == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		if (memcpy_s(_in_aes_iv, _len_aes_iv, _tmp_aes_iv, _len_aes_iv)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}

	}
	if (_tmp_encrypt_data != NULL && _len_encrypt_data != 0) {
		if ( _len_encrypt_data % sizeof(*_tmp_encrypt_data) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		_in_encrypt_data = (unsigned char*)malloc(_len_encrypt_data);
		if (_in_encrypt_data == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		if (memcpy_s(_in_encrypt_data, _len_encrypt_data, _tmp_encrypt_data, _len_encrypt_data)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}

	}
	_in_retval = ecall_Enqueue(__in_ms.ms_silo_id, __in_ms.ms_silo_beg_idx, __in_ms.ms_num, (const uint8_t*)_in_aes_key, (const uint8_t*)_in_aes_iv, (const unsigned char*)_in_encrypt_data, _tmp_data_size);
	if (memcpy_verw_s(&ms->ms_retval, sizeof(ms->ms_retval), &_in_retval, sizeof(_in_retval))) {
		status = SGX_ERROR_UNEXPECTED;
		goto err;
	}

err:
	if (_in_aes_key) free(_in_aes_key);
	if (_in_aes_iv) free(_in_aes_iv);
	if (_in_encrypt_data) free(_in_encrypt_data);
	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_OblivQueueHead(void* pms)
{
	CHECK_REF_POINTER(pms, sizeof(ms_ecall_OblivQueueHead_t));
	//
	// fence after pointer checks
	//
	sgx_lfence();
	ms_ecall_OblivQueueHead_t* ms = SGX_CAST(ms_ecall_OblivQueueHead_t*, pms);
	ms_ecall_OblivQueueHead_t __in_ms;
	if (memcpy_s(&__in_ms, sizeof(ms_ecall_OblivQueueHead_t), ms, sizeof(ms_ecall_OblivQueueHead_t))) {
		return SGX_ERROR_UNEXPECTED;
	}
	sgx_status_t status = SGX_SUCCESS;
	int* _tmp_idx = __in_ms.ms_idx;
	size_t _len_idx = 1 * sizeof(int);
	int* _in_idx = NULL;
	int* _tmp_silo_id = __in_ms.ms_silo_id;
	size_t _len_silo_id = 1 * sizeof(int);
	int* _in_silo_id = NULL;

	if (sizeof(*_tmp_idx) != 0 &&
		1 > (SIZE_MAX / sizeof(*_tmp_idx))) {
		return SGX_ERROR_INVALID_PARAMETER;
	}

	if (sizeof(*_tmp_silo_id) != 0 &&
		1 > (SIZE_MAX / sizeof(*_tmp_silo_id))) {
		return SGX_ERROR_INVALID_PARAMETER;
	}

	CHECK_UNIQUE_POINTER(_tmp_idx, _len_idx);
	CHECK_UNIQUE_POINTER(_tmp_silo_id, _len_silo_id);

	//
	// fence after pointer checks
	//
	sgx_lfence();

	if (_tmp_idx != NULL && _len_idx != 0) {
		if ( _len_idx % sizeof(*_tmp_idx) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		if ((_in_idx = (int*)malloc(_len_idx)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_idx, 0, _len_idx);
	}
	if (_tmp_silo_id != NULL && _len_silo_id != 0) {
		if ( _len_silo_id % sizeof(*_tmp_silo_id) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		if ((_in_silo_id = (int*)malloc(_len_silo_id)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_silo_id, 0, _len_silo_id);
	}
	ecall_OblivQueueHead(_in_idx, _in_silo_id);
	if (_in_idx) {
		if (memcpy_verw_s(_tmp_idx, _len_idx, _in_idx, _len_idx)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}
	}
	if (_in_silo_id) {
		if (memcpy_verw_s(_tmp_silo_id, _len_silo_id, _in_silo_id, _len_silo_id)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}
	}

err:
	if (_in_idx) free(_in_idx);
	if (_in_silo_id) free(_in_silo_id);
	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_OblivFreeQueue(void* pms)
{
	sgx_status_t status = SGX_SUCCESS;
	if (pms != NULL) return SGX_ERROR_INVALID_PARAMETER;
	ecall_OblivFreeQueue();
	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_GetSiloIndex(void* pms)
{
	CHECK_REF_POINTER(pms, sizeof(ms_ecall_GetSiloIndex_t));
	//
	// fence after pointer checks
	//
	sgx_lfence();
	ms_ecall_GetSiloIndex_t* ms = SGX_CAST(ms_ecall_GetSiloIndex_t*, pms);
	ms_ecall_GetSiloIndex_t __in_ms;
	if (memcpy_s(&__in_ms, sizeof(ms_ecall_GetSiloIndex_t), ms, sizeof(ms_ecall_GetSiloIndex_t))) {
		return SGX_ERROR_UNEXPECTED;
	}
	sgx_status_t status = SGX_SUCCESS;
	int _in_retval;


	_in_retval = ecall_GetSiloIndex(__in_ms.ms_silo_id);
	if (memcpy_verw_s(&ms->ms_retval, sizeof(ms->ms_retval), &_in_retval, sizeof(_in_retval))) {
		status = SGX_ERROR_UNEXPECTED;
		goto err;
	}

err:
	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_GetDataIndex(void* pms)
{
	CHECK_REF_POINTER(pms, sizeof(ms_ecall_GetDataIndex_t));
	//
	// fence after pointer checks
	//
	sgx_lfence();
	ms_ecall_GetDataIndex_t* ms = SGX_CAST(ms_ecall_GetDataIndex_t*, pms);
	ms_ecall_GetDataIndex_t __in_ms;
	if (memcpy_s(&__in_ms, sizeof(ms_ecall_GetDataIndex_t), ms, sizeof(ms_ecall_GetDataIndex_t))) {
		return SGX_ERROR_UNEXPECTED;
	}
	sgx_status_t status = SGX_SUCCESS;
	int _in_retval;


	_in_retval = ecall_GetDataIndex(__in_ms.ms_data_id);
	if (memcpy_verw_s(&ms->ms_retval, sizeof(ms->ms_retval), &_in_retval, sizeof(_in_retval))) {
		status = SGX_ERROR_UNEXPECTED;
		goto err;
	}

err:
	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_OblivInitQueue(void* pms)
{
	CHECK_REF_POINTER(pms, sizeof(ms_ecall_OblivInitQueue_t));
	//
	// fence after pointer checks
	//
	sgx_lfence();
	ms_ecall_OblivInitQueue_t* ms = SGX_CAST(ms_ecall_OblivInitQueue_t*, pms);
	ms_ecall_OblivInitQueue_t __in_ms;
	if (memcpy_s(&__in_ms, sizeof(ms_ecall_OblivInitQueue_t), ms, sizeof(ms_ecall_OblivInitQueue_t))) {
		return SGX_ERROR_UNEXPECTED;
	}
	sgx_status_t status = SGX_SUCCESS;


	ecall_OblivInitQueue(__in_ms.ms_queue_size);


	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_OblivCreateQueue(void* pms)
{
	CHECK_REF_POINTER(pms, sizeof(ms_ecall_OblivCreateQueue_t));
	//
	// fence after pointer checks
	//
	sgx_lfence();
	ms_ecall_OblivCreateQueue_t* ms = SGX_CAST(ms_ecall_OblivCreateQueue_t*, pms);
	ms_ecall_OblivCreateQueue_t __in_ms;
	if (memcpy_s(&__in_ms, sizeof(ms_ecall_OblivCreateQueue_t), ms, sizeof(ms_ecall_OblivCreateQueue_t))) {
		return SGX_ERROR_UNEXPECTED;
	}
	sgx_status_t status = SGX_SUCCESS;


	ecall_OblivCreateQueue(__in_ms.ms_queue_size);


	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_OblivDequeue(void* pms)
{
	sgx_status_t status = SGX_SUCCESS;
	if (pms != NULL) return SGX_ERROR_INVALID_PARAMETER;
	ecall_OblivDequeue();
	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_OblivDequeueMore(void* pms)
{
	CHECK_REF_POINTER(pms, sizeof(ms_ecall_OblivDequeueMore_t));
	//
	// fence after pointer checks
	//
	sgx_lfence();
	ms_ecall_OblivDequeueMore_t* ms = SGX_CAST(ms_ecall_OblivDequeueMore_t*, pms);
	ms_ecall_OblivDequeueMore_t __in_ms;
	if (memcpy_s(&__in_ms, sizeof(ms_ecall_OblivDequeueMore_t), ms, sizeof(ms_ecall_OblivDequeueMore_t))) {
		return SGX_ERROR_UNEXPECTED;
	}
	sgx_status_t status = SGX_SUCCESS;


	ecall_OblivDequeueMore(__in_ms.ms_num);


	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_ClearInformation(void* pms)
{
	CHECK_REF_POINTER(pms, sizeof(ms_ecall_ClearInformation_t));
	//
	// fence after pointer checks
	//
	sgx_lfence();
	ms_ecall_ClearInformation_t* ms = SGX_CAST(ms_ecall_ClearInformation_t*, pms);
	ms_ecall_ClearInformation_t __in_ms;
	if (memcpy_s(&__in_ms, sizeof(ms_ecall_ClearInformation_t), ms, sizeof(ms_ecall_ClearInformation_t))) {
		return SGX_ERROR_UNEXPECTED;
	}
	sgx_status_t status = SGX_SUCCESS;


	ecall_ClearInformation(__in_ms.ms_silo_id);


	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_ImportInformation(void* pms)
{
	CHECK_REF_POINTER(pms, sizeof(ms_ecall_ImportInformation_t));
	//
	// fence after pointer checks
	//
	sgx_lfence();
	ms_ecall_ImportInformation_t* ms = SGX_CAST(ms_ecall_ImportInformation_t*, pms);
	ms_ecall_ImportInformation_t __in_ms;
	if (memcpy_s(&__in_ms, sizeof(ms_ecall_ImportInformation_t), ms, sizeof(ms_ecall_ImportInformation_t))) {
		return SGX_ERROR_UNEXPECTED;
	}
	sgx_status_t status = SGX_SUCCESS;
	const uint8_t* _tmp_aes_key = __in_ms.ms_aes_key;
	size_t _len_aes_key = 16 * sizeof(uint8_t);
	uint8_t* _in_aes_key = NULL;
	const uint8_t* _tmp_aes_iv = __in_ms.ms_aes_iv;
	size_t _len_aes_iv = 16 * sizeof(uint8_t);
	uint8_t* _in_aes_iv = NULL;
	const unsigned char* _tmp_encrypt_data = __in_ms.ms_encrypt_data;
	size_t _tmp_data_size = __in_ms.ms_data_size;
	size_t _len_encrypt_data = _tmp_data_size * sizeof(unsigned char);
	unsigned char* _in_encrypt_data = NULL;
	int _in_retval;

	if (sizeof(*_tmp_aes_key) != 0 &&
		16 > (SIZE_MAX / sizeof(*_tmp_aes_key))) {
		return SGX_ERROR_INVALID_PARAMETER;
	}

	if (sizeof(*_tmp_aes_iv) != 0 &&
		16 > (SIZE_MAX / sizeof(*_tmp_aes_iv))) {
		return SGX_ERROR_INVALID_PARAMETER;
	}

	if (sizeof(*_tmp_encrypt_data) != 0 &&
		(size_t)_tmp_data_size > (SIZE_MAX / sizeof(*_tmp_encrypt_data))) {
		return SGX_ERROR_INVALID_PARAMETER;
	}

	CHECK_UNIQUE_POINTER(_tmp_aes_key, _len_aes_key);
	CHECK_UNIQUE_POINTER(_tmp_aes_iv, _len_aes_iv);
	CHECK_UNIQUE_POINTER(_tmp_encrypt_data, _len_encrypt_data);

	//
	// fence after pointer checks
	//
	sgx_lfence();

	if (_tmp_aes_key != NULL && _len_aes_key != 0) {
		if ( _len_aes_key % sizeof(*_tmp_aes_key) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		_in_aes_key = (uint8_t*)malloc(_len_aes_key);
		if (_in_aes_key == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		if (memcpy_s(_in_aes_key, _len_aes_key, _tmp_aes_key, _len_aes_key)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}

	}
	if (_tmp_aes_iv != NULL && _len_aes_iv != 0) {
		if ( _len_aes_iv % sizeof(*_tmp_aes_iv) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		_in_aes_iv = (uint8_t*)malloc(_len_aes_iv);
		if (_in_aes_iv == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		if (memcpy_s(_in_aes_iv, _len_aes_iv, _tmp_aes_iv, _len_aes_iv)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}

	}
	if (_tmp_encrypt_data != NULL && _len_encrypt_data != 0) {
		if ( _len_encrypt_data % sizeof(*_tmp_encrypt_data) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		_in_encrypt_data = (unsigned char*)malloc(_len_encrypt_data);
		if (_in_encrypt_data == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		if (memcpy_s(_in_encrypt_data, _len_encrypt_data, _tmp_encrypt_data, _len_encrypt_data)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}

	}
	_in_retval = ecall_ImportInformation(__in_ms.ms_silo_id, __in_ms.ms_importType, _tmp_data_size, __in_ms.ms_query_k, (const uint8_t*)_in_aes_key, (const uint8_t*)_in_aes_iv, (const unsigned char*)_in_encrypt_data);
	if (memcpy_verw_s(&ms->ms_retval, sizeof(ms->ms_retval), &_in_retval, sizeof(_in_retval))) {
		status = SGX_ERROR_UNEXPECTED;
		goto err;
	}

err:
	if (_in_aes_key) free(_in_aes_key);
	if (_in_aes_iv) free(_in_aes_iv);
	if (_in_encrypt_data) free(_in_encrypt_data);
	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_JointEstimation(void* pms)
{
	CHECK_REF_POINTER(pms, sizeof(ms_ecall_JointEstimation_t));
	//
	// fence after pointer checks
	//
	sgx_lfence();
	ms_ecall_JointEstimation_t* ms = SGX_CAST(ms_ecall_JointEstimation_t*, pms);
	ms_ecall_JointEstimation_t __in_ms;
	if (memcpy_s(&__in_ms, sizeof(ms_ecall_JointEstimation_t), ms, sizeof(ms_ecall_JointEstimation_t))) {
		return SGX_ERROR_UNEXPECTED;
	}
	sgx_status_t status = SGX_SUCCESS;


	ecall_JointEstimation(__in_ms.ms_silo_num, __in_ms.ms_k);


	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_GetPrunedK(void* pms)
{
	CHECK_REF_POINTER(pms, sizeof(ms_ecall_GetPrunedK_t));
	//
	// fence after pointer checks
	//
	sgx_lfence();
	ms_ecall_GetPrunedK_t* ms = SGX_CAST(ms_ecall_GetPrunedK_t*, pms);
	ms_ecall_GetPrunedK_t __in_ms;
	if (memcpy_s(&__in_ms, sizeof(ms_ecall_GetPrunedK_t), ms, sizeof(ms_ecall_GetPrunedK_t))) {
		return SGX_ERROR_UNEXPECTED;
	}
	sgx_status_t status = SGX_SUCCESS;
	const uint8_t* _tmp_aes_key = __in_ms.ms_aes_key;
	size_t _len_aes_key = 16 * sizeof(uint8_t);
	uint8_t* _in_aes_key = NULL;
	const uint8_t* _tmp_aes_iv = __in_ms.ms_aes_iv;
	size_t _len_aes_iv = 16 * sizeof(uint8_t);
	uint8_t* _in_aes_iv = NULL;
	unsigned char* _tmp_encrypt_k = __in_ms.ms_encrypt_k;
	size_t _tmp_max_output_size = __in_ms.ms_max_output_size;
	size_t _len_encrypt_k = _tmp_max_output_size * sizeof(unsigned char);
	unsigned char* _in_encrypt_k = NULL;
	int _in_retval;

	if (sizeof(*_tmp_aes_key) != 0 &&
		16 > (SIZE_MAX / sizeof(*_tmp_aes_key))) {
		return SGX_ERROR_INVALID_PARAMETER;
	}

	if (sizeof(*_tmp_aes_iv) != 0 &&
		16 > (SIZE_MAX / sizeof(*_tmp_aes_iv))) {
		return SGX_ERROR_INVALID_PARAMETER;
	}

	if (sizeof(*_tmp_encrypt_k) != 0 &&
		(size_t)_tmp_max_output_size > (SIZE_MAX / sizeof(*_tmp_encrypt_k))) {
		return SGX_ERROR_INVALID_PARAMETER;
	}

	CHECK_UNIQUE_POINTER(_tmp_aes_key, _len_aes_key);
	CHECK_UNIQUE_POINTER(_tmp_aes_iv, _len_aes_iv);
	CHECK_UNIQUE_POINTER(_tmp_encrypt_k, _len_encrypt_k);

	//
	// fence after pointer checks
	//
	sgx_lfence();

	if (_tmp_aes_key != NULL && _len_aes_key != 0) {
		if ( _len_aes_key % sizeof(*_tmp_aes_key) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		_in_aes_key = (uint8_t*)malloc(_len_aes_key);
		if (_in_aes_key == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		if (memcpy_s(_in_aes_key, _len_aes_key, _tmp_aes_key, _len_aes_key)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}

	}
	if (_tmp_aes_iv != NULL && _len_aes_iv != 0) {
		if ( _len_aes_iv % sizeof(*_tmp_aes_iv) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		_in_aes_iv = (uint8_t*)malloc(_len_aes_iv);
		if (_in_aes_iv == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		if (memcpy_s(_in_aes_iv, _len_aes_iv, _tmp_aes_iv, _len_aes_iv)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}

	}
	if (_tmp_encrypt_k != NULL && _len_encrypt_k != 0) {
		if ( _len_encrypt_k % sizeof(*_tmp_encrypt_k) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		if ((_in_encrypt_k = (unsigned char*)malloc(_len_encrypt_k)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_encrypt_k, 0, _len_encrypt_k);
	}
	_in_retval = ecall_GetPrunedK(__in_ms.ms_silo_id, _tmp_max_output_size, (const uint8_t*)_in_aes_key, (const uint8_t*)_in_aes_iv, _in_encrypt_k);
	if (memcpy_verw_s(&ms->ms_retval, sizeof(ms->ms_retval), &_in_retval, sizeof(_in_retval))) {
		status = SGX_ERROR_UNEXPECTED;
		goto err;
	}
	if (_in_encrypt_k) {
		if (memcpy_verw_s(_tmp_encrypt_k, _len_encrypt_k, _in_encrypt_k, _len_encrypt_k)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}
	}

err:
	if (_in_aes_key) free(_in_aes_key);
	if (_in_aes_iv) free(_in_aes_iv);
	if (_in_encrypt_k) free(_in_encrypt_k);
	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_CandRefinement(void* pms)
{
	CHECK_REF_POINTER(pms, sizeof(ms_ecall_CandRefinement_t));
	//
	// fence after pointer checks
	//
	sgx_lfence();
	ms_ecall_CandRefinement_t* ms = SGX_CAST(ms_ecall_CandRefinement_t*, pms);
	ms_ecall_CandRefinement_t __in_ms;
	if (memcpy_s(&__in_ms, sizeof(ms_ecall_CandRefinement_t), ms, sizeof(ms_ecall_CandRefinement_t))) {
		return SGX_ERROR_UNEXPECTED;
	}
	sgx_status_t status = SGX_SUCCESS;


	ecall_CandRefinement(__in_ms.ms_silo_num, __in_ms.ms_k);


	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_CandRefinementBase(void* pms)
{
	CHECK_REF_POINTER(pms, sizeof(ms_ecall_CandRefinementBase_t));
	//
	// fence after pointer checks
	//
	sgx_lfence();
	ms_ecall_CandRefinementBase_t* ms = SGX_CAST(ms_ecall_CandRefinementBase_t*, pms);
	ms_ecall_CandRefinementBase_t __in_ms;
	if (memcpy_s(&__in_ms, sizeof(ms_ecall_CandRefinementBase_t), ms, sizeof(ms_ecall_CandRefinementBase_t))) {
		return SGX_ERROR_UNEXPECTED;
	}
	sgx_status_t status = SGX_SUCCESS;


	ecall_CandRefinementBase(__in_ms.ms_silo_num, __in_ms.ms_k);


	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_GetThres(void* pms)
{
	CHECK_REF_POINTER(pms, sizeof(ms_ecall_GetThres_t));
	//
	// fence after pointer checks
	//
	sgx_lfence();
	ms_ecall_GetThres_t* ms = SGX_CAST(ms_ecall_GetThres_t*, pms);
	ms_ecall_GetThres_t __in_ms;
	if (memcpy_s(&__in_ms, sizeof(ms_ecall_GetThres_t), ms, sizeof(ms_ecall_GetThres_t))) {
		return SGX_ERROR_UNEXPECTED;
	}
	sgx_status_t status = SGX_SUCCESS;
	const uint8_t* _tmp_aes_key = __in_ms.ms_aes_key;
	size_t _len_aes_key = 16 * sizeof(uint8_t);
	uint8_t* _in_aes_key = NULL;
	const uint8_t* _tmp_aes_iv = __in_ms.ms_aes_iv;
	size_t _len_aes_iv = 16 * sizeof(uint8_t);
	uint8_t* _in_aes_iv = NULL;
	unsigned char* _tmp_encrypt_thres = __in_ms.ms_encrypt_thres;
	size_t _tmp_max_output_size = __in_ms.ms_max_output_size;
	size_t _len_encrypt_thres = _tmp_max_output_size * sizeof(unsigned char);
	unsigned char* _in_encrypt_thres = NULL;
	int _in_retval;

	if (sizeof(*_tmp_aes_key) != 0 &&
		16 > (SIZE_MAX / sizeof(*_tmp_aes_key))) {
		return SGX_ERROR_INVALID_PARAMETER;
	}

	if (sizeof(*_tmp_aes_iv) != 0 &&
		16 > (SIZE_MAX / sizeof(*_tmp_aes_iv))) {
		return SGX_ERROR_INVALID_PARAMETER;
	}

	if (sizeof(*_tmp_encrypt_thres) != 0 &&
		(size_t)_tmp_max_output_size > (SIZE_MAX / sizeof(*_tmp_encrypt_thres))) {
		return SGX_ERROR_INVALID_PARAMETER;
	}

	CHECK_UNIQUE_POINTER(_tmp_aes_key, _len_aes_key);
	CHECK_UNIQUE_POINTER(_tmp_aes_iv, _len_aes_iv);
	CHECK_UNIQUE_POINTER(_tmp_encrypt_thres, _len_encrypt_thres);

	//
	// fence after pointer checks
	//
	sgx_lfence();

	if (_tmp_aes_key != NULL && _len_aes_key != 0) {
		if ( _len_aes_key % sizeof(*_tmp_aes_key) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		_in_aes_key = (uint8_t*)malloc(_len_aes_key);
		if (_in_aes_key == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		if (memcpy_s(_in_aes_key, _len_aes_key, _tmp_aes_key, _len_aes_key)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}

	}
	if (_tmp_aes_iv != NULL && _len_aes_iv != 0) {
		if ( _len_aes_iv % sizeof(*_tmp_aes_iv) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		_in_aes_iv = (uint8_t*)malloc(_len_aes_iv);
		if (_in_aes_iv == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		if (memcpy_s(_in_aes_iv, _len_aes_iv, _tmp_aes_iv, _len_aes_iv)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}

	}
	if (_tmp_encrypt_thres != NULL && _len_encrypt_thres != 0) {
		if ( _len_encrypt_thres % sizeof(*_tmp_encrypt_thres) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		if ((_in_encrypt_thres = (unsigned char*)malloc(_len_encrypt_thres)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_encrypt_thres, 0, _len_encrypt_thres);
	}
	_in_retval = ecall_GetThres(__in_ms.ms_silo_id, _tmp_max_output_size, (const uint8_t*)_in_aes_key, (const uint8_t*)_in_aes_iv, _in_encrypt_thres);
	if (memcpy_verw_s(&ms->ms_retval, sizeof(ms->ms_retval), &_in_retval, sizeof(_in_retval))) {
		status = SGX_ERROR_UNEXPECTED;
		goto err;
	}
	if (_in_encrypt_thres) {
		if (memcpy_verw_s(_tmp_encrypt_thres, _len_encrypt_thres, _in_encrypt_thres, _len_encrypt_thres)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}
	}

err:
	if (_in_aes_key) free(_in_aes_key);
	if (_in_aes_iv) free(_in_aes_iv);
	if (_in_encrypt_thres) free(_in_encrypt_thres);
	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_TopkSelection(void* pms)
{
	CHECK_REF_POINTER(pms, sizeof(ms_ecall_TopkSelection_t));
	//
	// fence after pointer checks
	//
	sgx_lfence();
	ms_ecall_TopkSelection_t* ms = SGX_CAST(ms_ecall_TopkSelection_t*, pms);
	ms_ecall_TopkSelection_t __in_ms;
	if (memcpy_s(&__in_ms, sizeof(ms_ecall_TopkSelection_t), ms, sizeof(ms_ecall_TopkSelection_t))) {
		return SGX_ERROR_UNEXPECTED;
	}
	sgx_status_t status = SGX_SUCCESS;


	ecall_TopkSelection(__in_ms.ms_silo_num, __in_ms.ms_k);


	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_GetK(void* pms)
{
	CHECK_REF_POINTER(pms, sizeof(ms_ecall_GetK_t));
	//
	// fence after pointer checks
	//
	sgx_lfence();
	ms_ecall_GetK_t* ms = SGX_CAST(ms_ecall_GetK_t*, pms);
	ms_ecall_GetK_t __in_ms;
	if (memcpy_s(&__in_ms, sizeof(ms_ecall_GetK_t), ms, sizeof(ms_ecall_GetK_t))) {
		return SGX_ERROR_UNEXPECTED;
	}
	sgx_status_t status = SGX_SUCCESS;
	int _in_retval;


	_in_retval = ecall_GetK(__in_ms.ms_silo_id);
	if (memcpy_verw_s(&ms->ms_retval, sizeof(ms->ms_retval), &_in_retval, sizeof(_in_retval))) {
		status = SGX_ERROR_UNEXPECTED;
		goto err;
	}

err:
	return status;
}

SGX_EXTERNC const struct {
	size_t nr_ecall;
	struct {void* ecall_addr; uint8_t is_priv; uint8_t is_switchless;} ecall_table[18];
} g_ecall_table = {
	18,
	{
		{(void*)(uintptr_t)sgx_ecall_Enqueue, 0, 0},
		{(void*)(uintptr_t)sgx_ecall_OblivQueueHead, 0, 0},
		{(void*)(uintptr_t)sgx_ecall_OblivFreeQueue, 0, 0},
		{(void*)(uintptr_t)sgx_ecall_GetSiloIndex, 0, 0},
		{(void*)(uintptr_t)sgx_ecall_GetDataIndex, 0, 0},
		{(void*)(uintptr_t)sgx_ecall_OblivInitQueue, 0, 0},
		{(void*)(uintptr_t)sgx_ecall_OblivCreateQueue, 0, 0},
		{(void*)(uintptr_t)sgx_ecall_OblivDequeue, 0, 0},
		{(void*)(uintptr_t)sgx_ecall_OblivDequeueMore, 0, 0},
		{(void*)(uintptr_t)sgx_ecall_ClearInformation, 0, 0},
		{(void*)(uintptr_t)sgx_ecall_ImportInformation, 0, 0},
		{(void*)(uintptr_t)sgx_ecall_JointEstimation, 0, 0},
		{(void*)(uintptr_t)sgx_ecall_GetPrunedK, 0, 0},
		{(void*)(uintptr_t)sgx_ecall_CandRefinement, 0, 0},
		{(void*)(uintptr_t)sgx_ecall_CandRefinementBase, 0, 0},
		{(void*)(uintptr_t)sgx_ecall_GetThres, 0, 0},
		{(void*)(uintptr_t)sgx_ecall_TopkSelection, 0, 0},
		{(void*)(uintptr_t)sgx_ecall_GetK, 0, 0},
	}
};

SGX_EXTERNC const struct {
	size_t nr_ocall;
	uint8_t entry_table[6][18];
} g_dyn_entry_table = {
	6,
	{
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, },
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, },
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, },
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, },
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, },
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, },
	}
};


sgx_status_t SGX_CDECL ocall_print_string(const char* str)
{
	sgx_status_t status = SGX_SUCCESS;
	size_t _len_str = str ? strlen(str) + 1 : 0;

	ms_ocall_print_string_t* ms = NULL;
	size_t ocalloc_size = sizeof(ms_ocall_print_string_t);
	void *__tmp = NULL;


	CHECK_ENCLAVE_POINTER(str, _len_str);

	if (ADD_ASSIGN_OVERFLOW(ocalloc_size, (str != NULL) ? _len_str : 0))
		return SGX_ERROR_INVALID_PARAMETER;

	__tmp = sgx_ocalloc(ocalloc_size);
	if (__tmp == NULL) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}
	ms = (ms_ocall_print_string_t*)__tmp;
	__tmp = (void *)((size_t)__tmp + sizeof(ms_ocall_print_string_t));
	ocalloc_size -= sizeof(ms_ocall_print_string_t);

	if (str != NULL) {
		if (memcpy_verw_s(&ms->ms_str, sizeof(const char*), &__tmp, sizeof(const char*))) {
			sgx_ocfree();
			return SGX_ERROR_UNEXPECTED;
		}
		if (_len_str % sizeof(*str) != 0) {
			sgx_ocfree();
			return SGX_ERROR_INVALID_PARAMETER;
		}
		if (memcpy_verw_s(__tmp, ocalloc_size, str, _len_str)) {
			sgx_ocfree();
			return SGX_ERROR_UNEXPECTED;
		}
		__tmp = (void *)((size_t)__tmp + _len_str);
		ocalloc_size -= _len_str;
	} else {
		ms->ms_str = NULL;
	}

	status = sgx_ocall(0, ms);

	if (status == SGX_SUCCESS) {
	}
	sgx_ocfree();
	return status;
}

sgx_status_t SGX_CDECL sgx_oc_cpuidex(int cpuinfo[4], int leaf, int subleaf)
{
	sgx_status_t status = SGX_SUCCESS;
	size_t _len_cpuinfo = 4 * sizeof(int);

	ms_sgx_oc_cpuidex_t* ms = NULL;
	size_t ocalloc_size = sizeof(ms_sgx_oc_cpuidex_t);
	void *__tmp = NULL;

	void *__tmp_cpuinfo = NULL;

	CHECK_ENCLAVE_POINTER(cpuinfo, _len_cpuinfo);

	if (ADD_ASSIGN_OVERFLOW(ocalloc_size, (cpuinfo != NULL) ? _len_cpuinfo : 0))
		return SGX_ERROR_INVALID_PARAMETER;

	__tmp = sgx_ocalloc(ocalloc_size);
	if (__tmp == NULL) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}
	ms = (ms_sgx_oc_cpuidex_t*)__tmp;
	__tmp = (void *)((size_t)__tmp + sizeof(ms_sgx_oc_cpuidex_t));
	ocalloc_size -= sizeof(ms_sgx_oc_cpuidex_t);

	if (cpuinfo != NULL) {
		if (memcpy_verw_s(&ms->ms_cpuinfo, sizeof(int*), &__tmp, sizeof(int*))) {
			sgx_ocfree();
			return SGX_ERROR_UNEXPECTED;
		}
		__tmp_cpuinfo = __tmp;
		if (_len_cpuinfo % sizeof(*cpuinfo) != 0) {
			sgx_ocfree();
			return SGX_ERROR_INVALID_PARAMETER;
		}
		memset_verw(__tmp_cpuinfo, 0, _len_cpuinfo);
		__tmp = (void *)((size_t)__tmp + _len_cpuinfo);
		ocalloc_size -= _len_cpuinfo;
	} else {
		ms->ms_cpuinfo = NULL;
	}

	if (memcpy_verw_s(&ms->ms_leaf, sizeof(ms->ms_leaf), &leaf, sizeof(leaf))) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}

	if (memcpy_verw_s(&ms->ms_subleaf, sizeof(ms->ms_subleaf), &subleaf, sizeof(subleaf))) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}

	status = sgx_ocall(1, ms);

	if (status == SGX_SUCCESS) {
		if (cpuinfo) {
			if (memcpy_s((void*)cpuinfo, _len_cpuinfo, __tmp_cpuinfo, _len_cpuinfo)) {
				sgx_ocfree();
				return SGX_ERROR_UNEXPECTED;
			}
		}
	}
	sgx_ocfree();
	return status;
}

sgx_status_t SGX_CDECL sgx_thread_wait_untrusted_event_ocall(int* retval, const void* self)
{
	sgx_status_t status = SGX_SUCCESS;

	ms_sgx_thread_wait_untrusted_event_ocall_t* ms = NULL;
	size_t ocalloc_size = sizeof(ms_sgx_thread_wait_untrusted_event_ocall_t);
	void *__tmp = NULL;


	__tmp = sgx_ocalloc(ocalloc_size);
	if (__tmp == NULL) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}
	ms = (ms_sgx_thread_wait_untrusted_event_ocall_t*)__tmp;
	__tmp = (void *)((size_t)__tmp + sizeof(ms_sgx_thread_wait_untrusted_event_ocall_t));
	ocalloc_size -= sizeof(ms_sgx_thread_wait_untrusted_event_ocall_t);

	if (memcpy_verw_s(&ms->ms_self, sizeof(ms->ms_self), &self, sizeof(self))) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}

	status = sgx_ocall(2, ms);

	if (status == SGX_SUCCESS) {
		if (retval) {
			if (memcpy_s((void*)retval, sizeof(*retval), &ms->ms_retval, sizeof(ms->ms_retval))) {
				sgx_ocfree();
				return SGX_ERROR_UNEXPECTED;
			}
		}
	}
	sgx_ocfree();
	return status;
}

sgx_status_t SGX_CDECL sgx_thread_set_untrusted_event_ocall(int* retval, const void* waiter)
{
	sgx_status_t status = SGX_SUCCESS;

	ms_sgx_thread_set_untrusted_event_ocall_t* ms = NULL;
	size_t ocalloc_size = sizeof(ms_sgx_thread_set_untrusted_event_ocall_t);
	void *__tmp = NULL;


	__tmp = sgx_ocalloc(ocalloc_size);
	if (__tmp == NULL) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}
	ms = (ms_sgx_thread_set_untrusted_event_ocall_t*)__tmp;
	__tmp = (void *)((size_t)__tmp + sizeof(ms_sgx_thread_set_untrusted_event_ocall_t));
	ocalloc_size -= sizeof(ms_sgx_thread_set_untrusted_event_ocall_t);

	if (memcpy_verw_s(&ms->ms_waiter, sizeof(ms->ms_waiter), &waiter, sizeof(waiter))) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}

	status = sgx_ocall(3, ms);

	if (status == SGX_SUCCESS) {
		if (retval) {
			if (memcpy_s((void*)retval, sizeof(*retval), &ms->ms_retval, sizeof(ms->ms_retval))) {
				sgx_ocfree();
				return SGX_ERROR_UNEXPECTED;
			}
		}
	}
	sgx_ocfree();
	return status;
}

sgx_status_t SGX_CDECL sgx_thread_setwait_untrusted_events_ocall(int* retval, const void* waiter, const void* self)
{
	sgx_status_t status = SGX_SUCCESS;

	ms_sgx_thread_setwait_untrusted_events_ocall_t* ms = NULL;
	size_t ocalloc_size = sizeof(ms_sgx_thread_setwait_untrusted_events_ocall_t);
	void *__tmp = NULL;


	__tmp = sgx_ocalloc(ocalloc_size);
	if (__tmp == NULL) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}
	ms = (ms_sgx_thread_setwait_untrusted_events_ocall_t*)__tmp;
	__tmp = (void *)((size_t)__tmp + sizeof(ms_sgx_thread_setwait_untrusted_events_ocall_t));
	ocalloc_size -= sizeof(ms_sgx_thread_setwait_untrusted_events_ocall_t);

	if (memcpy_verw_s(&ms->ms_waiter, sizeof(ms->ms_waiter), &waiter, sizeof(waiter))) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}

	if (memcpy_verw_s(&ms->ms_self, sizeof(ms->ms_self), &self, sizeof(self))) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}

	status = sgx_ocall(4, ms);

	if (status == SGX_SUCCESS) {
		if (retval) {
			if (memcpy_s((void*)retval, sizeof(*retval), &ms->ms_retval, sizeof(ms->ms_retval))) {
				sgx_ocfree();
				return SGX_ERROR_UNEXPECTED;
			}
		}
	}
	sgx_ocfree();
	return status;
}

sgx_status_t SGX_CDECL sgx_thread_set_multiple_untrusted_events_ocall(int* retval, const void** waiters, size_t total)
{
	sgx_status_t status = SGX_SUCCESS;
	size_t _len_waiters = total * sizeof(void*);

	ms_sgx_thread_set_multiple_untrusted_events_ocall_t* ms = NULL;
	size_t ocalloc_size = sizeof(ms_sgx_thread_set_multiple_untrusted_events_ocall_t);
	void *__tmp = NULL;


	CHECK_ENCLAVE_POINTER(waiters, _len_waiters);

	if (ADD_ASSIGN_OVERFLOW(ocalloc_size, (waiters != NULL) ? _len_waiters : 0))
		return SGX_ERROR_INVALID_PARAMETER;

	__tmp = sgx_ocalloc(ocalloc_size);
	if (__tmp == NULL) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}
	ms = (ms_sgx_thread_set_multiple_untrusted_events_ocall_t*)__tmp;
	__tmp = (void *)((size_t)__tmp + sizeof(ms_sgx_thread_set_multiple_untrusted_events_ocall_t));
	ocalloc_size -= sizeof(ms_sgx_thread_set_multiple_untrusted_events_ocall_t);

	if (waiters != NULL) {
		if (memcpy_verw_s(&ms->ms_waiters, sizeof(const void**), &__tmp, sizeof(const void**))) {
			sgx_ocfree();
			return SGX_ERROR_UNEXPECTED;
		}
		if (_len_waiters % sizeof(*waiters) != 0) {
			sgx_ocfree();
			return SGX_ERROR_INVALID_PARAMETER;
		}
		if (memcpy_verw_s(__tmp, ocalloc_size, waiters, _len_waiters)) {
			sgx_ocfree();
			return SGX_ERROR_UNEXPECTED;
		}
		__tmp = (void *)((size_t)__tmp + _len_waiters);
		ocalloc_size -= _len_waiters;
	} else {
		ms->ms_waiters = NULL;
	}

	if (memcpy_verw_s(&ms->ms_total, sizeof(ms->ms_total), &total, sizeof(total))) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}

	status = sgx_ocall(5, ms);

	if (status == SGX_SUCCESS) {
		if (retval) {
			if (memcpy_s((void*)retval, sizeof(*retval), &ms->ms_retval, sizeof(ms->ms_retval))) {
				sgx_ocfree();
				return SGX_ERROR_UNEXPECTED;
			}
		}
	}
	sgx_ocfree();
	return status;
}

