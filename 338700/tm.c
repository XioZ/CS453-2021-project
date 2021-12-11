/**
 * @file   tm.c
 * @author [...]
 *
 * @section LICENSE
 *
 * [...]
 *
 * @section DESCRIPTION
 *
 * Implementation of your own transaction manager.
 * You can completely rewrite this file (and create more files) as you wish.
 * Only the interface (i.e. exported symbols and semantic) must be preserved.
 **/

// Requested features
#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L
#ifdef __STDC_NO_ATOMICS__
#error Current C11 compiler does not support atomic operations
#endif

// External headers

// Internal headers
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <tm.h>

#include "macros.h"

typedef struct segment_t {
  struct segment_t* prev;
  struct segment_t* next;
} segment_t;

typedef struct shared_region_t {  // region data and metadata
  void* start;
  segment_t* segments;
  size_t size;
  size_t alignment;
} shared_region_t;

/** Create (i.e. allocate + init) a new shared memory region, with one first
 *non-free-able allocated segment (todo: to implement in tm_free())
 of the requested size and alignment.
 * - can be called concurrently (not accessing any shared variable)
 * @param size  Size of the first shared segment of memory to allocate (in
 *bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared
 * memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
 **/
shared_t tm_create(size_t unused(size), size_t unused(align)) {
  // TODO: need to validate 'align' is power of 2 and
  // 'size' is multiple of 'align'??
  // allocate memory for region struct, which will be used as region handle
  shared_region_t* region = (shared_region_t*)malloc(sizeof(shared_region_t));
  if (region == NULL) {
    return invalid_shared;
  }
  // allocate memory of requested size and alignment for region data
  if (posix_memalign(&(region->start), align, size) != 0) {
    // if fails, free the handle
    free(region);
    return invalid_shared;
  }
  // TODO: initialize the locks that guard the shared variables
  // used by the STM library

  // initialize first segment to 0
  memset(region->start, 0, size);
  // initialize region metadata
  region->segments = NULL;  // the library hasn't allocated any segment yet
  region->size = size;
  region->alignment = align;
  // return pointer to region struct as handle
  return region;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * - no concurrent calls for the same region (thus not accessing any shared
 * variables)
 * @param shared Shared memory region to destroy (hasn't been destroyed), with
 * no running/pending transaction (till function returns)
 **/
void tm_destroy(shared_t unused(shared)) {
  shared_region_t* region = (shared_region_t*)shared;
  // free region metadata: each segment handle
  while (region->segments != NULL) {
    segment_t* tail = region->segments->next;
    free(region->segments);
    region->segments = tail;
  }
  // free region data
  free(region->start);
  // free region handle
  free(region);
  // TODO: clean up locks used by the library
}

/** [thread-safe] Return the start address of the first allocated segment in the
 *shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment's first word
 **/
void* tm_start(shared_t unused(shared)) {
  // TODO: acquire & free lock on shared_region_t
  return ((shared_region_t*)shared)->start;
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of
 *the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
 **/
size_t tm_size(shared_t unused(shared)) {
  // TODO: acquire & free lock on shared_region_t
  return ((shared_region_t*)shared)->size;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the
 *given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
 **/
size_t tm_align(shared_t unused(shared)) {
  // TODO: acquire & free lock on shared_region_t
  return ((shared_region_t*)shared)->size;
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
 **/
tx_t tm_begin(shared_t unused(shared), bool unused(is_ro)) {
  // TODO: tm_begin(shared_t)
  return invalid_tx;
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
 **/
bool tm_end(shared_t unused(shared), tx_t unused(tx)) {
  // TODO: tm_end(shared_t, tx_t)
  return false;
}

/** [thread-safe] Read operation in the given transaction, source in the shared
 *region and target in a private region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in the shared region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the
 *alignment
 * @param target Target start address (in a private region)
 * @return Whether the whole transaction can continue
 **/
bool tm_read(shared_t unused(shared), tx_t unused(tx),
             void const* unused(source), size_t unused(size),
             void* unused(target)) {
  // TODO: tm_read(shared_t, tx_t, void const*, size_t, void*)
  return false;
}

/** [thread-safe] Write operation in the given transaction, source in a private
 *region and target in the shared region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in a private region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the
 *alignment
 * @param target Target start address (in the shared region)
 * @return Whether the whole transaction can continue
 **/
bool tm_write(shared_t unused(shared), tx_t unused(tx),
              void const* unused(source), size_t unused(size),
              void* unused(target)) {
  // TODO: tm_write(shared_t, tx_t, void const*, size_t, void*)
  return false;
}

/** [thread-safe] Memory allocation in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param size   Allocation requested size (in bytes), must be a positive
 *multiple of the alignment
 * @param target Pointer in private memory receiving the address of the first
 *byte of the newly allocated, aligned segment
 * @return Whether the whole transaction can continue (success/nomem), or not
 *(abort_alloc)
 **/
alloc_t tm_alloc(shared_t unused(shared), tx_t unused(tx), size_t unused(size),
                 void** unused(target)) {
  // TODO: tm_alloc(shared_t, tx_t, size_t, void**)
  return abort_alloc;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment
 *to deallocate
 * @return Whether the whole transaction can continue
 **/
bool tm_free(shared_t unused(shared), tx_t unused(tx), void* unused(target)) {
  // TODO: tm_free(shared_t, tx_t, void*)
  return false;
}
