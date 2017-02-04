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

#define BLOOM_BASE_SIZE 1024
#define DEFAULT_ERROR 0.003     // TODO
#define MIN_ERROR 0.0000000001  // TODO

bloom *bloomNew(void) {
	bloom *bf = zmalloc(sizeof(bloom) + BLOOM_BASE_SIZE);
	bf->numfilters = 1;
    bf->error = DEFAULT_ERROR;
	return bf;
}

void bloomRelease(bloom *bf) {
    zfree(bf);
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
        bf->error = error;
    } else if (!updated && bf->error != error) {
        addReplyErrorFormat(c,"cannot change error on existing bloom filter: %f %f", bf->error, error);
        return;
    }

    for (;j<c->argc;j++) {
        /* TODO: add elements */
    }

    if (updated) {
    	signalModifiedKey(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_STRING,"bfadd",c->argv[1],c->db->id);
        server.dirty++;
        //FIXME cache here HLL_INVALIDATE_CACHE(hdr);
    }
    addReply(c, updated ? shared.cone : shared.czero);
}
