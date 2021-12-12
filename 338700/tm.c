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
//    void *start;
    void *copy_a;
    void *copy_b;
    size_t size;
    word_control_t *word_controls;
    size_t num_words;
    // doubly linked list (simplify tm_free() when a segment in the middle is removed)
    struct segment_t *prev;
    struct segment_t *next;
} segment_t;

typedef struct shared_region_t {  // region data and metadata
    segment_t *segment_list;  // points to the first segment (start of segment metadata)
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
  // adjust alignment so a word can at least fit a pointer (memory address)
  size_t alignment = align < sizeof(struct segment_t *) ?
                     sizeof(void *) : align;
  region->alignment = alignment;
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
  // allocate memory for segment metadata + segment data
  segment_t *first_segment;
  size_t segment_length = sizeof(segment_t) + size;
  if (unlikely(posix_memalign(
    (void **) &(first_segment), alignment, segment_length)) != 0) {
    free(region);
    free(word_controls);
    return nomem_alloc;
  }
  // initialize segment metadata
  first_segment->size = size;
  first_segment->word_controls = word_controls;
  first_segment->num_words = num_words;
  // allocate memory for copy A and B
  if (unlikely(posix_memalign(
    (void **) &(first_segment->copy_a), align, size)) != 0) {
    free(region);
    free(word_controls);
    free(first_segment);
    return invalid_shared;
  }
  if (unlikely(posix_memalign(
    (void **) &(first_segment->copy_b), align, size)) != 0) {
    free(region);
    free(word_controls);
    free(first_segment);
    free(first_segment->copy_a);
    return invalid_shared;
  }
  // update region metadata: insert first segment at the front of the list
  first_segment->prev = NULL;
  first_segment->next = NULL;
  region->segment_list = first_segment;
  // calculate segment data address
  void *segment_data = (void *) ((uintptr_t) first_segment + sizeof(segment_t));
  // initialize segment data, copy A and B to 0
  memset(segment_data, 0, size);
  memset(first_segment->copy_a, 0, size);
  memset(first_segment->copy_b, 0, size);
  // return pointer to region struct as handle
  return region;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * - no concurrent calls for the same region (thus not accessing any shared
 * variables)
 * @param shared Shared memory region to destroy (hasn't been destroyed), with
 * no running/pending transaction (till function returns)
 **/
void tm_destroy(shared_t shared) {
  shared_region_t *region = (shared_region_t *) shared;
  // free every segment, its control structure and copies
  while (region->segment_list) {
    segment_t *tail = region->segment_list->next;
    free(region->segment_list->word_controls);
    free(region->segment_list->copy_a);
    free(region->segment_list->copy_b);
    free(region->segment_list);
    region->segment_list = tail;
  }
  // free region metadata
  free(region);
  // TODO: clean up locks
}

/** [thread-safe] Return the start address of the first allocated segment in the
 *shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment's first word
 **/
void *tm_start(shared_t shared) {
  // TODO: synchronize
  segment_t *segment_list = ((shared_region_t *) shared)->segment_list;
  return (void *) ((uintptr_t) segment_list + sizeof(segment_t));
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of
 *the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
 **/
size_t tm_size(shared_t shared) {
  // TODO: synchronize
  return ((shared_region_t *) shared)->segment_list->size;
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
  size_t alignment = ((shared_region_t *) shared)->alignment; // adjusted in tm_create() when first segment is allocated
  // allocate memory & initialize control struct for each word in the segment
  size_t num_words = size / alignment;
  word_control_t *word_controls = calloc(num_words, sizeof(word_control_t));
  if (unlikely(!word_controls)) {
    return nomem_alloc;
  }
  for (int i = 0; i < num_words; i++) {
    word_controls[i].valid_copy = 0; // copy A
    word_controls[i].is_written = 0; // unwritten
    word_controls[i].first_accessor = invalid_tx; // no txn
  }
  // allocate memory for segment metadata + segment data
  segment_t *new_segment;
  size_t segment_length = sizeof(segment_t) + size;
  if (unlikely(posix_memalign(
    (void **) &(new_segment), alignment, segment_length)) != 0) {
    free(word_controls);
    return nomem_alloc;
  }
  // initialize segment metadata
  new_segment->size = size;
  new_segment->word_controls = word_controls;
  new_segment->num_words = num_words;
  // allocate memory for copy A and B
  if (unlikely(posix_memalign(
    (void **) &(new_segment->copy_a), alignment, size)) != 0) {
    free(word_controls);
    free(new_segment);
    return nomem_alloc;
  }
  if (unlikely(posix_memalign(
    (void **) &(new_segment->copy_b), alignment, size)) != 0) {
    free(word_controls);
    free(new_segment);
    free(new_segment->copy_a);
    return nomem_alloc;
  }
  // update region metadata: insert new segment at the front of the list
  shared_region_t *region = (shared_region_t *) shared;
  new_segment->prev = NULL;
  new_segment->next = region->segment_list;
  if (new_segment->next) {
    new_segment->next->prev = new_segment;
  }
  region->segment_list = new_segment;
  // calculate segment data address
  void *segment_data = (void *) ((uintptr_t) new_segment + sizeof(segment_t));
  // initialize segment data, copy A and B to 0
  memset(segment_data, 0, size);
  memset(new_segment->copy_a, 0, size);
  memset(new_segment->copy_b, 0, size);
  // point user pointer to start of segment data
  *target = segment_data;
  return success_alloc;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment
 *to deallocate
 * @return Whether the whole transaction can continue
 **/
bool tm_free(shared_t shared, tx_t unused(tx), void *target) {
  // TODO: synchronize
  segment_t *segment =
    (struct segment_t *) ((uintptr_t) target - sizeof(segment_t));
  // update region metadata: remove from segment list
  if (segment->prev) {
    segment->prev->next = segment->next;
  } else {
    ((shared_region_t *) shared)->segment_list = segment->next;
  }
  if (segment->next) {
    segment->next->prev = segment->prev;
  }
  // TODO: free ONLY when last transaction in this batch leaves
  //  AND ONLY IF this transaction commits
  // implementation: 1) add segment pointer to list of segments to free in region
  //  2) when transaction commits, tag its segments as ok to free
  //  3) when last transaction leaves batcher, call free() on those pointers
  //  before incrementing epoch
//  free(segment);
  return true;
}
