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

typedef struct word_control_t {  // dual-version's control structure
    // 0 means A readable (valid) B writable,
    // 1 means A writable B readable (valid)
    unsigned int valid_copy: 1;  // 1 bit
    // 1 means written, 0 means not written but maybe read
    unsigned int is_written: 1;  // 1 bit
    // 1st read-write transaction that read/wrote this word
    tx_t first_accessor;
} word_control_t;

typedef struct segment_t {  // segment metadata
    // TODO: prev and next => easier to implement free()
    void *start;
    void *copy_a;
    void *copy_b;
    size_t size;
    word_control_t *word_controls;
    size_t num_words;
    struct segment_t *prev;
    struct segment_t *next;
} segment_t;

typedef struct shared_region_t {  // region data and metadata
    segment_t *segment_list;  // explicitly allocated segments (via tm_alloc())
    size_t alignment;     // alignment for all segments
} shared_region_t;

/** Create (i.e. allocate + init) a new shared memory region, with one first
 *non-free-able allocated segment of the requested size and alignment.
 * - can be called concurrently (not accessing any shared variable)
 * @param size  Size of the first shared memory segment to allocate (in
 *bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared
 * memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
 **/
shared_t tm_create(size_t unused(size), size_t unused(align)) {
  // TODO: synchronize and initialize locks
  // allocate memory & initialize region metadata (used as region handle)
  shared_region_t *region = (shared_region_t *) malloc(sizeof(shared_region_t));
  if (unlikely(!region)) {
    return invalid_shared;
  }
  region->alignment = align;
  // allocate memory & initialize control struct for each word
  // in the first unfreeable segment
  const num_words = size / align;
  word_control_t *word_controls = calloc(num_words, sizeof(word_control_t));
  if (unlikely(!word_controls)) {
    free(region);
    return invalid_shared;
  }
  for (int i = 0; i < num_words; i++) {
    word_controls[i].valid_copy = 0; // copy A
    word_controls[i].is_written = 0; // unwritten
    word_controls[i].first_accessor = invalid_tx; // no txn
  }
  // allocate memory & initialize segment metadata, copy A and B
  segment_t *segment = malloc(sizeof(segment_t));
  if (unlikely(!segment)) {
    free(region);
    free(word_controls);
    return invalid_shared;
  }
  segment->size = size;
  segment->word_controls = word_controls;
  segment->num_words = num_words;
  if (unlikely(posix_memalign(
          (void **) &(segment->copy_a), align, size)) != 0) {
    free(region);
    free(word_controls);
    free(segment);
    return invalid_shared;
  }
  if (unlikely(posix_memalign(
          (void **) &(segment->copy_b), align, size)) != 0) {
    free(region);
    free(word_controls);
    free(segment);
    free(segment->copy_a);
    return invalid_shared;
  }
  // allocate memory & initialize first segment data
  if (unlikely(posix_memalign(
          (void **) &(segment->start), align, size)) != 0) {
    free(region);
    free(word_controls);
    free(segment);
    free(segment->copy_a);
    free(segment->copy_b);
    return invalid_shared;
  }
  memset(segment->start, 0, size);
  // update shared region metadata: insert first segment at the front of the list
  segment->prev = NULL;
  segment->next = NULL;
  region->segment_list = segment;
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
  // TODO: free every word control structure of every segment
  // TODO: then every segment metadata
  // TODO: finally free region metadata
  shared_region_t *region = (shared_region_t *) shared;
  // free region metadata: each segment handle
  // while (region->segments != NULL) {
  //   segment_t* tail = region->segments->next;
  //   free(region->segments);
  //   region->segments = tail;
  // }
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
void *tm_start(shared_t shared) {
  // TODO: synchronize
  return ((shared_region_t *) shared)->segment_list[0].start;
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of
 *the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
 **/
size_t tm_size(shared_t shared) {
  // TODO: synchronize
  return ((shared_region_t *) shared)->segment_list[0].size;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the
 *given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
 **/
size_t tm_align(shared_t shared) {
  // TODO: synchronize
  return ((shared_region_t *) shared)->alignment;
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
 **/
tx_t tm_begin(shared_t unused(shared), bool unused(is_ro)) {
  // goal: 1) allow multiple read-only transactions to happen concurrently
  // (same as reference implementation), and
  // 2) allow multiple read-write transactions WITHOUT overlapping/conflicting
  // memory access to happen concurrently (diff/improvement from reference
  // 3) read-only transactions to happen while there're (concurrent to)
  // ongoing/pending read-write transactions

  // TODO: tm_begin(shared_t)

  // TODO: enter() batcher

  return invalid_tx;
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
 **/
bool tm_end(shared_t unused(shared), tx_t unused(tx)) {
  // TODO: tm_end(shared_t, tx_t)

  // TODO: commit()
  // TODO: leave() batcher

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
             void const *unused(source), size_t unused(size),
             void *unused(target)) {
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
              void const *unused(source), size_t unused(size),
              void *unused(target)) {
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
alloc_t tm_alloc(shared_t shared, tx_t unused(tx), size_t size,
                 void **target) {
  // TODO: synchronize
  // fft: need to set max(pointer size, region->alignment) as alignment?
  // no, not storing segment metadata (segment_t struct) in aligned memory
  // allocated by posix_memalign()
  const alignment = ((shared_region_t *) shared)->alignment;
  // allocate memory & initialize control struct for each word in the segment
  const num_words = size / alignment;
  word_control_t *word_controls = calloc(num_words, sizeof(word_control_t));
  if (unlikely(!word_controls)) {
    return nomem_alloc;
  }
  for (int i = 0; i < num_words; i++) {
    word_controls[i].valid_copy = 0; // copy A
    word_controls[i].is_written = 0; // unwritten
    word_controls[i].first_accessor = invalid_tx; // no txn
  }
  // allocate memory & initialize segment metadata, copy A and B
  segment_t *segment = malloc(sizeof(segment_t));
  if (unlikely(!segment)) {
    free(word_controls);
    return nomem_alloc;
  }
  segment->size = size;
  segment->word_controls = word_controls;
  segment->num_words = num_words;
  if (unlikely(posix_memalign(
          (void **) &(segment->copy_a), alignment, size)) != 0) {
    free(word_controls);
    free(segment);
    return nomem_alloc;
  }
  if (unlikely(posix_memalign(
          (void **) &(segment->copy_b), alignment, size)) != 0) {
    free(word_controls);
    free(segment);
    free(segment->copy_a);
    return nomem_alloc;
  }
  // allocate memory & initialize segment data
  if (unlikely(posix_memalign(
          (void **) &(segment->start), alignment, size)) != 0) {
    free(word_controls);
    free(segment);
    free(segment->copy_a);
    free(segment->copy_b);
    return nomem_alloc;
  }
  memset(segment->start, 0, size);
  // update shared region metadata: insert new segment at the front of the list
  shared_region_t *region = (shared_region_t *) shared;
  segment->prev = NULL;
  segment->next = region->segment_list;
  if (segment->next) {
    segment->next->prev = segment;
  }
  region->segment_list = segment;
  // point user pointer to start of segment data
  *target = segment->start;
  return abort_alloc;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment
 *to deallocate
 * @return Whether the whole transaction can continue
 **/
bool tm_free(shared_t unused(shared), tx_t unused(tx), void *unused(target)) {
  // TODO: tm_free(shared_t, tx_t, void*)
  return false;
}
