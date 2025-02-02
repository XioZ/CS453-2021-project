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
#include <stdatomic.h>

#include "macros.h"
#include "shared-lock.h"

static const unsigned int NO_TXN = 0;
static const unsigned int read_only_tx = 1;
static atomic_int transactions_counter = 2;

typedef struct transaction_t {
    int id;
    bool is_ro;
} transaction_t;

typedef struct word_control_t {  // dual-version's control structure
    bool is_a_valid;
    bool is_written;
    // 1st read-write transaction that read/wrote this word
    int first_accessor; // txn id
} word_control_t;

typedef struct segment_t {  // segment metadata
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
    struct shared_lock_t lock;  // Global (coarse-grained) lock
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
  const int num_words = size / align;
  word_control_t *word_controls = calloc(num_words, sizeof(word_control_t));
  if (unlikely(!word_controls)) {
    free(region);
    return invalid_shared;
  }
  for (int i = 0; i < num_words; i++) {
    word_controls[i].is_a_valid = true; // copy A
    word_controls[i].is_written = false; // unwritten
    word_controls[i].first_accessor = NO_TXN; // no txn
  }
  // allocate memory for segment metadata + segment data
  segment_t *first_segment;
  size_t segment_length = sizeof(segment_t) + size;
  if (unlikely(posix_memalign(
    (void **) &(first_segment), alignment, segment_length)) != 0) {
    free(region);
    free(word_controls);
    return invalid_shared;
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
  if (!shared_lock_init(&(region->lock))) {
    free(region);
    free(word_controls);
    free(first_segment);
    free(first_segment->copy_a);
    free(first_segment->copy_b);
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
  shared_lock_cleanup(&(region->lock));
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
tx_t tm_begin(shared_t unused(shared), bool is_ro) {
  // goal: 1) allow multiple read-only transactions to happen concurrently
  // (same as reference implementation), and
  // 2) allow multiple read-write transactions WITHOUT overlapping/conflicting
  // memory access to happen concurrently (diff/improvement from reference
  // 3) read-only transactions to happen while there are (concurrent to)
  // ongoing/pending read-write transactions
  // TODO: enter() batcher
  // TODO: synchronize
  transaction_t *transaction = malloc(sizeof(transaction_t)); // TODO: free
  if (is_ro) {
    if (unlikely(
    // allows multiple (read-only) txns to acquire the lock concurrently
      !shared_lock_acquire_shared(&(((shared_region_t *) shared)->lock))))
      return invalid_tx;
    transaction->id = read_only_tx;
    transaction->is_ro = true;
  } else {
    // allows only 1 (read-write) txn to acquire the lock at a time
    if (unlikely(!shared_lock_acquire(&(((shared_region_t *) shared)->lock))))
      return invalid_tx;
    transaction->id = atomic_load(&transactions_counter);
    transaction->is_ro = false;
    atomic_fetch_add(&transactions_counter, 1);
  }
  return (uintptr_t) transaction;
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
 **/
bool tm_end(shared_t shared, tx_t tx) {
  // TODO: commit() ?
  // TODO: leave() batcher
  // TODO: synchronize
  transaction_t *transaction = (transaction_t *) tx;
  if (transaction->id == read_only_tx) {
    shared_lock_release_shared(&(((shared_region_t *) shared)->lock));
  } else {
    shared_lock_release(&(((shared_region_t *) shared)->lock));
  }
  free(transaction);
  return true;
}

bool read_word(int index, void *target, size_t alignment,
  transaction_t *transaction, segment_t *segment) {
  word_control_t *word = &segment->word_controls[index];
  void *readable_copy = word->is_a_valid ? segment->copy_a : segment->copy_b;
  void *writable_copy = word->is_a_valid ? segment->copy_b : segment->copy_a;
  if (transaction->is_ro) {
    memcpy(target, readable_copy, alignment);
    return true;
  } else {
    if (word->is_written) {
      if (transaction->id == word->first_accessor) {
        // word's been written (writable copy) by this transaction itself
        memcpy(target, writable_copy, alignment);
        return true;
      } else { // other transaction has written this word (writable copy), must abort
        return false;
      }
    } else { // word hasn't been written, but may have been read (accessed)
      memcpy(target, readable_copy, alignment);
      if (word->first_accessor == NO_TXN) { // word's neither written nor read
        word->first_accessor = transaction->id;
      }
      return true;
    }
  } // end-else
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
bool tm_read(shared_t shared, tx_t tx,
  void const *source, size_t size, void *target) {
  // TODO: synchronize
  size_t alignment = ((shared_region_t *) shared)->alignment;
  int index_start = *(int *) source;
  int index_end = *(int *) (source + size);
  segment_t *segment = (segment_t *) ((uintptr_t) source
    - index_start * alignment - sizeof(segment_t));
  for (int index = index_start; index < index_end; index++) {
    void *target_for_index = (void *) ((uintptr_t) target + index * alignment);
    bool can_continue = read_word(index, target_for_index, alignment,
      (transaction_t *) tx, segment);
    if (!can_continue) {
      return false;
    }
  }
  return true;
}

bool write_word(void *source, int index, size_t alignment,
  transaction_t *transaction, segment_t *segment) {
  word_control_t *word = &segment->word_controls[index];
  void *writable_copy = word->is_a_valid ? segment->copy_b : segment->copy_a;
  if (word->is_written) {
    if (transaction->id == word->first_accessor) { // word's been written by me
      memcpy(writable_copy, source, alignment);
      return true;
    } else { // other transaction has written this word (writable copy), must abort
      return false;
    }
  } else { // word hasn't been written, but may have been read (accessed)
    if (word->first_accessor != NO_TXN
      && word->first_accessor != transaction->id) {
      // word's been read by other txn, must abort
      return false;
    } else {
      // word's never been read or been read by myself
      memcpy(writable_copy, source, alignment);
      word->first_accessor = transaction->id;
      word->is_written = true;
      return true;
    }
  } // end-else
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
bool tm_write(shared_t shared, tx_t tx,
  void const *source, size_t size,
  void *target) {
  // TODO: synchronize
  size_t alignment = ((shared_region_t *) shared)->alignment;
  int index_start = *(int *) target;
  int index_end = *(int *) (target + size);
  segment_t *segment = (segment_t *) ((uintptr_t) target
    - index_start * alignment - sizeof(segment_t));
  for (int index = index_start; index < index_end; index++) {
    void *source_for_index = (void *) ((uintptr_t) source + index * alignment);
    bool can_continue = write_word(source_for_index, index, alignment,
      (transaction_t *) tx, segment);
    if (!can_continue) {
      return false;
    }
  }
  return true;
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
  unsigned int num_words = size / alignment;
  word_control_t *word_controls = calloc(num_words, sizeof(word_control_t));
  if (unlikely(!word_controls)) {
    return nomem_alloc;
  }
  for (unsigned int i = 0; i < num_words; i++) {
    word_controls[i].is_a_valid = true; // copy A
    word_controls[i].is_written = false; // unwritten
    word_controls[i].first_accessor = NO_TXN; // no txn
  }
  // allocate memory for segment metadata and indices
  segment_t *segment;
  size_t segment_length = sizeof(segment_t) + size;
  if (unlikely(posix_memalign(
    (void **) &(segment), alignment, segment_length)) != 0) {
    free(word_controls);
    return nomem_alloc;
  }
  // initialize segment metadata
  segment->size = size;
  segment->word_controls = word_controls;
  segment->num_words = num_words;
  // allocate memory for copy A, B
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
  // update region metadata: insert new segment at the front of the list
  shared_region_t *region = (shared_region_t *) shared;
  segment->prev = NULL;
  segment->next = region->segment_list;
  if (segment->next) {
    segment->next->prev = segment;
  }
  region->segment_list = segment;
  // calculate address to start of segment indices
  void *segment_indices = (void *) ((uintptr_t) segment + sizeof(segment_t));
  // initialize indices
  for (unsigned int index = 0; index < num_words; index++) {
    int *index_p = (int *) ((uintptr_t) segment_indices + alignment * index);
    *index_p = index;
  }
  // initialize copy A and B to 0
  memset(segment->copy_a, 0, size);
  memset(segment->copy_b, 0, size);
  // point user pointer to start of indices
  *target = segment_indices;
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
  return true;
  // TODO: can return true directly, free all at once in tm_destroy(),
  //  no need to flag segment as freed in the meantime
  //  (grader doesn't check, i.e. no use after free)
  // TODO: free ONLY when last transaction in this batch leaves
  //  AND ONLY IF this transaction commits
  // implementation: 1) add segment pointer to list of segments to free in region
  //  2) when transaction commits, tag its segments as ok to free
  //  3) when last transaction leaves batcher, call free() on those pointers
  //  before incrementing epoch
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
  free(segment);
  return true;
}
