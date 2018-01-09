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

/* Initial desired size of the bloom filter (in bytes). */
#define CONFIG_BLOOM_BASESIZE 2048

/* Default false-positive error rate */
#define CONFIG_BLOOM_DEFAULTERROR 0.003

/* Fill ratio of a filter before it is considered full. We also call this P */
#define CONFIG_BLOOM_DESIREDFILLRATIO 0.5

/* Desired growth for items, for each new allocated filter.
 * Default is 2.0, which means that each new filter should hold
 * twice as many items as the previous one. */
#define CONFIG_BLOOM_ITEMGROWRATIO    2.0

/* Desired tightening ratio for false-positive error.
 * Each new filter must have a tighten error ratio compared to
 * the previous one, to asymptotically approach the user-requested
 * ratio. */
#define CONFIG_BLOOM_TIGHTENINGRATIO  0.85

#define MIN_ERROR 0.0000000001  // TODO

/*
 * Bloom user-provided parameters:
 *
 * E = false probability ratio. This is used directly on the first filter
 *     and subsequente filters are computed so that the composed ratio
 *     does not diverge. We compute the following sequence:
 *
 *     e0 = E
 *     e[i] = e0 * CONFIG_BLOOM_TIGHTENINGRATIO^i
 *
 *
 * N = number of items that we want to store in each filter. This is
 *     a sequence of numbers (one for each filter), that is computed
 *     given the requested initial size in bytes, and the item growth ratio
 *
 *     n0 = CONFIG_BLOOM_BASESIZE*8 * (log(P)*log(1-P) / abs(log(e0)))
 *     n[i] = n0 * CONFIG_BLOOM_ITEMGROWRATIO^i
 *
 *
 * Parameters computed for each filter:
 *
 * K = number of partitions (aka hash functions)
 *        k[i] = ceil(log2(e[i]^-1))
 * M = size in bits of the filter
 *        m[i] = n[i] / ((log(P) * log(1-P)) / abs(log(e[i])))
 * S = size of each partition, in bits
 *        s[i] = m[i] / k[i]
 * C = number of elements that can be added before reaching the fill ratio
 *        c[i] = s[i] * log(1 / (1-P))
 *
 */

/* Calculate M for the current filter (we do not store it) */
#define FILTER_CALC_M(flt)  (flt->s * flt->k)

/* Calculate current fill ratio for the current filter */
#define FILTER_FILLRATIO(flt)  (1.0 - exp(-(double)flt->c / (double)FILTER_CALC_S(flt)))


filter* bloomFilterNew(bloom *bf) {
    int idx = bf->numfilters;

    /* Compute N0 (N for the first filter) so that the first M (memory size)
     * will match BLOOM_BASE_SIZE. */
    uint32_t n0 = CONFIG_BLOOM_BASESIZE*8 * ((log(CONFIG_BLOOM_DESIREDFILLRATIO) * log(1-CONFIG_BLOOM_DESIREDFILLRATIO)) / fabs(log(bf->e)));

    /* Compute E0 (E for the first filter) so that the composed probability converges to E. */
    double e0 = bf->e * (1 - CONFIG_BLOOM_TIGHTENINGRATIO) * 2;

    /* Compute input parameters for the new filter, iterating exponentially
     * given the configured ratios. */
    uint32_t n = n0 * pow(CONFIG_BLOOM_ITEMGROWRATIO, idx);
    double e = e0 * pow(CONFIG_BLOOM_TIGHTENINGRATIO, idx);

    /* Compute derived parameters */
    int k = ceil(-log2(e));
    uint64_t m = (double)n / ((log(CONFIG_BLOOM_DESIREDFILLRATIO) * log(1-CONFIG_BLOOM_DESIREDFILLRATIO)) / fabs(log(e)));
    uint64_t s = m / k;
    uint64_t bmax = (s*k) * CONFIG_BLOOM_DESIREDFILLRATIO;

    filter *flt = zmalloc(sizeof(filter) + k*sizeof(void*));
    flt->next = NULL;
    flt->encoding = 0;
    flt->s = s;
    flt->k = k;
    flt->b = 0;
    flt->bmax = bmax;
    for (int i=0;i<k;i++) {
        uint32_t bsize = (flt->s + 7) / 8;
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

uint64_t bloomFilterHash(unsigned char *ele, size_t elesize) {
    return MurmurHash64A(ele,elesize,0xc5fb9af2ULL);
}

int bloomFilterAdd(filter *flt, unsigned char *ele, size_t elesize) {
    /* Calculate initial hash for the element. Since 32-bit index is enough,
       we use a 64-bit hash as two 32-bit hashes. */
    uint64_t hash = bloomFilterHash(ele,elesize);
    uint32_t a = (uint32_t)hash;
    uint32_t b = (uint32_t)(hash>>32);
    int nbits = 0;

    for (unsigned int i=0;i<flt->k;i++) {
        /* To compute multiple hash functions from two hashes,
         * we use the algorithm described here:
         * http://www.eecs.harvard.edu/~michaelm/postscripts/rsa2008.pdf
         */
        uint64_t index = a;
        a += b; b += i;

        /* Use fast unbiased modulo reduction, instead of "% size".
         * See http://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/ */
        index = (index * flt->s) >> 32;

        /* Turn on the correct bit in each partition */
        nbits += (flt->parts[i][index>>3] & (1 << (index&7))) ? 0 : 1;
        flt->parts[i][index>>3] |= 1 << (index&7);
    }

    /* Keep count of total bits set to 1 */
    flt->b += nbits;

    /* If at least one bit was turned off, we consider the item really added. */
    return nbits > 0;
}

int bloomFilterExist(filter *flt, uint64_t hash) {
    uint32_t a = (uint32_t)hash;
    uint32_t b = (uint32_t)(hash>>32);

    for (unsigned int i=0;i<flt->k;i++) {
        /* See bloomFilterAdd for a description of the index calculation algorithm. */
        uint64_t index = a;
        a += b; b += i;
        index = (index * flt->s) >> 32;

        /* For each bit, if it's not set, early exit immediately */
        if (~(flt->parts[i][index>>3] >> (index & 7)) & 1)
            return 0;
    }
    return 1;
}

bloom *bloomNew(void) {
	bloom *bf = zmalloc(sizeof(bloom));
	bf->numfilters = 0;
    bf->e = CONFIG_BLOOM_DEFAULTERROR;
    bf->first = NULL; /* do not create filter here, because user might change error */
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

int bloomAdd(bloom *bf, unsigned char *ele, size_t elesize) {
    /* Go to the last filter, which is the current one */
    filter *flt = bf->first;
    if (!flt)
        flt = bf->first = bloomFilterNew(bf);
    else {
        while (flt->next)
            flt = flt->next;

        /* Check if this bloom filter is full; if so, allocate
         * a new one */
        if (flt->b >= flt->bmax) {
            flt->next = bloomFilterNew(bf);
            flt = flt->next;
        }
    }

    /* Add the element to the filter */
    return bloomFilterAdd(flt, ele, elesize);
}

int bloomExist(bloom *bf, unsigned char *ele, size_t elesize) {
    /* Calculate initial hash for the element */
    uint64_t hash = bloomFilterHash(ele,elesize);

    /* Check all bloom filters for membership. If the element is
       found in any of them, it means it's present. */
    for (filter *flt = bf->first; flt; flt = flt->next)
        if (bloomFilterExist(flt,hash))
            return 1;
    return 0;
}

int bloomCard(bloom *bf) {
    int nelem = 0;

    /* Sum cardinality estimation of each filter. */
    for (filter *flt = bf->first; flt; flt = flt->next) {
        double p = ((double)flt->b / (double)flt->bmax) * CONFIG_BLOOM_DESIREDFILLRATIO;
        nelem += floor((double)flt->s * -log(1.0-p) + .5);
    }
    return nelem;
}

/* ------------ Commands -------------------------- */

/* BFADD var [ERROR x] ELEMENTS ele ele .... ele => nadded */
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

    int nadded = 0;
    for (;j<c->argc;j++) {
        nadded += bloomAdd(bf,c->argv[j]->ptr,sdslen(c->argv[j]->ptr));
        updated++;
    }

    if (updated) {
    	signalModifiedKey(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_STRING,"bfadd",c->argv[1],c->db->id);
        server.dirty++;
        //FIXME cache here HLL_INVALIDATE_CACHE(hdr);
    }
    addReplyLongLong(c, nadded);
}

/* BFEXIST key elem -> 0/1 */
void bfexistCommand(client *c) {
    robj *o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL) {
        /* No bloom filter, treat as empty */
        addReply(c,shared.czero);
        return;
    } else if (checkType(c,o,OBJ_BLOOM))
        return;

    bloom *bf = (bloom*)o->ptr;
    int exist = bloomExist(bf,c->argv[2]->ptr,sdslen(c->argv[2]->ptr));
    addReply(c, exist ? shared.cone : shared.czero);
}

/* BFCOUNT key -> nelem */
void bfcountCommand(client *c) {
    robj *o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL) {
        /* No bloom filter, treat as empty */
        addReply(c,shared.czero);
        return;
    } else if (checkType(c,o,OBJ_BLOOM))
        return;

    bloom *bf = (bloom*)o->ptr;
    int ncard = bloomCard(bf);
    addReplyLongLong(c, ncard);
}

/* BFDEBUG <subcommand> <key> ... args ...
 * Various debugging functions about the bloom filters */
void bfdebugCommand(client *c) {
    char *cmd = c->argv[1]->ptr;
    robj *o = lookupKeyWrite(c->db,c->argv[2]);
    if (o == NULL) {
        addReplyError(c,"The specified key does not exist");
        return;
    } else if (checkType(c,o,OBJ_BLOOM))
        return;
    bloom *bf = (bloom*)o->ptr;

    /* BFDEBUG STATUS <key> */
    if (!strcasecmp(cmd, "status")) {
        if (c->argc != 3) goto arityerr;
        sds result = sdsempty();
        result = sdscatprintf(result,"n:%d e:%g", bf->numfilters, bf->e);
        addReplyBulkCBuffer(c,result,sdslen(result));
        sdsfree(result);

    /* BFDEBUG FILTER <key> <index> */
    } else if (!strcasecmp(cmd, "filter")) {
        long idx;
        if (c->argc != 4) goto arityerr;
        if (getLongFromObjectOrReply(c,c->argv[3],&idx,"invalid filter index") != 0) return;
        if (idx < 0) {
            addReplyError(c,"index out of range");
            return;
        }
        filter *flt = bf->first;
        for (;idx>0;idx--) {
            flt = flt->next;
            if (!flt) {
                addReplyError(c,"index out of range");
                return;
            }
        }

        sds result = sdsempty();
        result = sdscatprintf(result,"k:%u s:%llu b:%llu", flt->k, flt->s, flt->b);
        addReplyBulkCBuffer(c,result,sdslen(result));
        sdsfree(result);

    /* invalid command */
    } else
        addReplyErrorFormat(c,"Unknown BFDEBUG subcommand '%s'", cmd);
    return;

arityerr:
    addReplyErrorFormat(c,
        "Wrong number of arguments for the '%s' subcommand",cmd);
}

