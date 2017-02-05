/* bloom.c - Bloom filter probabilistic set membership.
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

#include "server.h"
#include <math.h>

#define BLOOM_BASE_SIZE 1024
#define DEFAULT_ERROR 0.003     // TODO
#define MIN_ERROR 0.0000000001  // TODO
#define DESIRED_FILL_RATIO 0.5
#define ITEMGROW_RATIO    2.0
#define TIGHTENING_RATIO  0.85

/*
 * Bloom parameters:
 *
 * E = false probability ratio (user-provided)
 * P = desired fill ratio
 *        p = 0.5
 * N = how many number of items do we want to store in the
 *     first filter?
 *        N = 128
 * K = number of hash functions
 *        k = log2(e^-1)
 * M = best size in bits for the filter for the given parameters
 *        m = n / ((log(p) * log(1-p)) / abs(log(e)))
 * S = size of each partition, in bits
 *        s = m / k
 */

#define FILTER_CALC_S(flt)  (flt->m / flt->k)


filter* bloomFilterNew(bloom *bf) {
    int idx = bf->numfilters;
    double e = bf->e * pow(TIGHTENING_RATIO, idx);
    uint32_t n = BLOOM_BASE_SIZE * pow(ITEMGROW_RATIO, idx);
    int k = ceil(log2(1.0 / e));
    uint64_t m = (double)n / ((log(DESIRED_FILL_RATIO) * log(1-DESIRED_FILL_RATIO)) / fabs(log(e)));

    filter *flt = zmalloc(sizeof(filter) + k*sizeof(void*));
    flt->next = NULL;
    flt->encoding = 0;
    flt->k = k;
    flt->c = 0;
    flt->m = m;
    for (int i=0;i<k;i++) {
        uint32_t bsize = (FILTER_CALC_S(flt) + 7) / 8;
        flt->parts[i] = zmalloc(bsize);
        memset(flt->parts[i],0,bsize);
    }
    bf->numfilters++;
    return flt;
}

void bloomFilterRelease(filter *flt) {
    for (unsigned int i=0;i<flt->k;i++)
        zfree(flt->parts[i]);
    zfree(flt);
}

// TODO: move to .h
uint64_t MurmurHash64A (const void * key, int len, unsigned int seed);


typedef uint64_t filterIndexIter;

uint32_t bloomFilterCalcIndex(filter *flt, uint64_t hash, int nidx) {
    /* To compute multiple hash functions from a single hash,
     * we use the algorithm described here:
     * http://www.eecs.harvard.edu/~michaelm/postscripts/rsa2008.pdf
     *
     * WHich is easier done than said: we split the hash in two
     * 32-bit parts (A and B), and then H(i) = A + B*i
     */
    uint32_t a = (uint32_t)hash;
    uint32_t b = (uint32_t)(hash>>32);
    uint64_t idx = a + b*nidx;

    /* Use fast unbiased modulo reduction, instead of "% size".
     * See http://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/ */
    uint64_t size = FILTER_CALC_S(flt);
    return (idx * size) >> 32;
}

uint64_t bloomFilterHash(unsigned char *ele, size_t elesize) {
    return MurmurHash64A(ele,elesize,0xc5fb9af2ULL);
}

void bloomFilterAdd(filter *flt, unsigned char *ele, size_t elesize) {
    /* Calculate initial hash for the element */
    uint64_t hash = bloomFilterHash(ele,elesize);

    for (unsigned int i=0;i<flt->k;i++) {
        /* Calculate and turn on bit for each partition */
        uint32_t index = bloomFilterCalcIndex(flt, hash, i);
        flt->parts[i][index>>3] |= 1 << (index&7);
    }
    flt->c++;
}

double bloomFilterFillRatio(filter *flt) {
    return 1.0 - exp(-(double)flt->c / (double)FILTER_CALC_S(flt));
}

bloom *bloomNew(void) {
	bloom *bf = zmalloc(sizeof(bloom));
	bf->numfilters = 0;
    bf->e = DEFAULT_ERROR;
    bf->first = bloomFilterNew(bf);
	return bf;
}

void bloomRelease(bloom *bf) {
    filter *flt = bf->first;
    while (flt) {
        filter *next = flt->next;
        bloomFilterRelease(flt);
        flt = next;
    }
    zfree(bf);
}

void bloomAdd(bloom *bf, unsigned char *ele, size_t elesize) {
    /* Go to the last filter, which is the current one */
    filter *flt = bf->first;
    while (flt->next)
        flt = flt->next;

    /* Check if this bloom filter is full; if so, allocate
     * a new one */
    if (bloomFilterFillRatio(flt) >= DESIRED_FILL_RATIO) {
        flt->next = bloomFilterNew(bf);
        flt = flt->next;
    }

    /* Add the element to the filter */
    bloomFilterAdd(flt, ele, elesize);
}

/* BFADD var [ERROR x] ELEMENTS ele ele .... ele => TODO */
void bfaddCommand(client *c) {
    int j=2; double error=0;
    while (j<c->argc) {
        if (!strcasecmp(c->argv[j]->ptr,"elements")) {
            j++;
            break;
        } else if (!strcasecmp(c->argv[j]->ptr,"error")) {
            if (j+1>=c->argc) {
                addReplyError(c,"no error specified");
                return;
            }
            if (getDoubleFromObjectOrReply(c,c->argv[j+1],&error,NULL) != C_OK) return;
            if (error < MIN_ERROR) {
                addReplyError(c,"error too small");
                return;
            }
            j+=2;
        } else {
            addReplyErrorFormat(c,"invalid option: %s",c->argv[j]->ptr);
            return;
        }
    }

    robj *o = lookupKeyWrite(c->db,c->argv[1]);
    int updated = 0;

    if (o == NULL) {
        o = createBloomObject();
        dbAdd(c->db,c->argv[1],o);
        updated++;
    } else if (checkType(c,o,OBJ_BLOOM))
        return;

    bloom *bf = (bloom*)o->ptr;
    if (updated && error != 0) {
        /* if the bloom filter was just created, and an error was specified,
           set it overriding the default. */
        bf->e = error;
    } else if (!updated && error != 0 &&  bf->e != error) {
        addReplyError(c,"cannot change error on existing bloom filter");
        return;
    }

    for (;j<c->argc;j++) {
        bloomAdd(bf,c->argv[j]->ptr,sdslen(c->argv[j]->ptr));
        updated++;
    }

    if (updated) {
    	signalModifiedKey(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_STRING,"bfadd",c->argv[1],c->db->id);
        server.dirty++;
        //FIXME cache here HLL_INVALIDATE_CACHE(hdr);
    }
    addReply(c, updated ? shared.cone : shared.czero);
}
