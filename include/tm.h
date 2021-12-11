/**
 * @file   tm.h
 * @author Sébastien ROUAULT <sebastien.rouault@epfl.ch>
 * @author Antoine MURAT <antoine.murat@epfl.ch>
 *
 * @section LICENSE
 *
 * Copyright © 2018-2021 Sébastien ROUAULT.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 3 of the License, or
 * any later version. Please see https://gnu.org/licenses/gpl.html
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * @section DESCRIPTION
 *
 * Interface declaration for the transaction manager to use (C version).
 * YOU SHOULD NOT MODIFY THIS FILE.
 **/

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

// -------------------------------------------------------------------------- //

typedef void* shared_t;  // The type of a shared memory region
static shared_t const invalid_shared = NULL;  // Invalid shared memory region

typedef struct word_t {  // dual-version's control structure
  void* copy_a;
  void* copy_b;
  // 0 means A readable (valid) B writable,
  // 1 means A writable B readable (valid)
  unsigned int valid_copy : 1;  // 1 bit
  // 1 means written, 0 means not written but maybe read
  unsigned int is_written : 1;  // 1 bit
  // 1st read-write transaction that read/wrote this word
  tx_t first_accessor;
} word_t;

typedef struct segment_t {  // segment metadata
  void* start;
  size_t size;
  word_t* words;
  size_t num_words;
} segment_t;

typedef struct shared_region_t {  // region data and metadata
  void* start;                    // first un-freeable segment
  size_t size;                    // size of first segment
  segment_t* segments;  // explicitly allocated segments (via tm_alloc())
  size_t alignment;     // alignment for all segments
} shared_region_t;

// Note: a uintptr_t is an unsigned integer that is big enough to store an
// address. Said differently, you can either use an integer to identify
// transactions, or an address (e.g., if you created an associated data
// structure).
typedef uintptr_t tx_t;  // The type of a transaction identifier
static tx_t const invalid_tx = ~((tx_t)0);  // Invalid transaction constant

typedef int alloc_t;
static alloc_t const success_alloc =
    0;  // Allocation successful and the TX can continue
static alloc_t const abort_alloc = 1;  // TX was aborted and could be retried
static alloc_t const nomem_alloc =
    2;  // Memory allocation failed but TX was not aborted

// -------------------------------------------------------------------------- //

shared_t tm_create(size_t, size_t);
void tm_destroy(shared_t);
void* tm_start(shared_t);
size_t tm_size(shared_t);
size_t tm_align(shared_t);
tx_t tm_begin(shared_t, bool);
bool tm_end(shared_t, tx_t);
bool tm_read(shared_t, tx_t, void const*, size_t, void*);
bool tm_write(shared_t, tx_t, void const*, size_t, void*);
alloc_t tm_alloc(shared_t, tx_t, size_t, void**);
bool tm_free(shared_t, tx_t, void*);
