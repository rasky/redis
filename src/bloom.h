/* bloom.h - Bloom filter probabilistic set membership.
 * This file implements the algorithm and the exported Redis commands.
 *
 * Copyright (c) 2017, Giovanni Bajo <giovannibajo at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef __BLOOM_H
#define __BLOOM_H
#include <stdint.h>

typedef struct filter {
    struct filter *next;  /* next filter in chain */
    uint64_t encoding;    /* encoding bitset: 1 bit per partition */
    uint64_t s;           /* size of each partition in bits */
    uint64_t b;           /* number of bits set in this filter */
    uint64_t bmax;        /* maximum number of bits that should be set */
    uint32_t k;           /* number of partitions */
    uint8_t *parts[];
} filter;

typedef struct bloom {
	double e;
	int numfilters;
	filter *first;
} bloom;

bloom *bloomNew(void);
void bloomRelease(bloom *bf);

#endif // __BLOOM_H
