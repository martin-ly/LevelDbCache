/*  =====================================================================
 *  clone - client-side Clone Pattern class

-------------------------------------------------------------------------
Copyright (c) 1991-2013 Andre Charles Legendre <andre.legendre@kalimasystems.org>
Copyright other contributors as noted in the AUTHORS file.

This file is based on zeroMQ zguide examples <https://github.com/imatix/zguide> 

This file is part of LevelDbCache, the shared in memory cache for levelDb Key Value store.

This is free software; you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by
the Free Software Foundation; either version 3 of the License, or (at
your option) any later version.

This software is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this program. If not, see
<http://www.gnu.org/licenses/>.
*  ===================================================================== */

#ifndef __CLONE_INCLUDED__
#define __CLONE_INCLUDED__

#include "kvmsg.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef void (__cdecl *PRETURNUNCALLBACENDSNAPSHOT)( char *key, char* value);
typedef void (__cdecl *PRETURNUNCALLBACKUPDATE)( char *key, char* value);

//  Structure of our class

struct _clone_t {
	zctx_t *ctx;                //  Our context wrapper
	void *pipe;                 //  Pipe through to clone agent
	void *logpipe;                 //  Pipe through to clone log agent
	PRETURNUNCALLBACENDSNAPSHOT pReturnCallbcksnapshot;
	PRETURNUNCALLBACKUPDATE pReturnCallbckupdate;
};

//  Opaque class structure
typedef struct _clone_t clone_t;

_EXPORTS_API void launchServer (int argc, char* confPath);
_EXPORTS_API clone_t *clone_new (char *confPath);
_EXPORTS_API void clone_destroy (clone_t **clone_p);
_EXPORTS_API void clone_subtree (clone_t *clone, char *subtree);
_EXPORTS_API void clone_connect_server (clone_t *clone, char *address, char *service);
_EXPORTS_API void clone_connect (clone_t *clone);
_EXPORTS_API void clone_set (clone_t *clone, char *cacheidstr, char *key, char *value, int ttl);
_EXPORTS_API char *clone_get (clone_t *clone, char *cacheidstr, char *key);
_EXPORTS_API void clone_logString (int level, int type, char *body);
_EXPORTS_API void __cdecl AddListnerForSnapshot(clone_t *clone,PRETURNUNCALLBACKUPDATE pReturnSnapshotCallback);
_EXPORTS_API void __cdecl AddListnerForUpdate(clone_t *clone,PRETURNUNCALLBACKUPDATE pReturnUpdateCallback);

#ifdef __cplusplus
}
#endif

#endif
