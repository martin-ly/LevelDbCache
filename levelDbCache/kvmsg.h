/*  =====================================================================
 *  kvmsg - key-value message class for example applications
 *  ===================================================================== */

#ifndef __KVMSG_H_INCLUDED__
#define __KVMSG_H_INCLUDED__

#define _EXPORTS_API __declspec(dllexport)

#include "czmq.h"

//  Opaque class structure
typedef struct _kvmsg kvmsg_t;

#ifdef __cplusplus
extern "C" {
#endif

//  Constructor, sets sequence as provided
_EXPORTS_API kvmsg_t *
    kvmsg_new (int64_t sequence);
//  Destructor
_EXPORTS_API void
    kvmsg_destroy (kvmsg_t **kvmsg_p);
//  Create duplicate of kvmsg
_EXPORTS_API kvmsg_t *
    kvmsg_dup (kvmsg_t *kvmsg);

//  Reads key-value message from socket, returns new kvmsg instance.
_EXPORTS_API kvmsg_t *
    kvmsg_recv (void *socket);
//  Send key-value message to socket; any empty frames are sent as such.
_EXPORTS_API void
    kvmsg_send (kvmsg_t *kvmsg, void *socket);

//  Return key from last read message, if any, else NULL
_EXPORTS_API char *
    kvmsg_key (kvmsg_t *kvmsg);
//  Return sequence nbr from last read message, if any
_EXPORTS_API int64_t
    kvmsg_sequence (kvmsg_t *kvmsg);
//  Return UUID from last read message, if any, else NULL
_EXPORTS_API byte *
    kvmsg_uuid (kvmsg_t *kvmsg);
//  Return body from last read message, if any, else NULL
_EXPORTS_API byte *
    kvmsg_body (kvmsg_t *kvmsg);
//  Return body size from last read message, if any, else zero
_EXPORTS_API size_t
    kvmsg_size (kvmsg_t *kvmsg);

//  Set message key as provided
_EXPORTS_API void
    kvmsg_set_key (kvmsg_t *kvmsg, char *key);
//  Set message sequence number
_EXPORTS_API void
    kvmsg_set_sequence (kvmsg_t *kvmsg, int64_t sequence);
//  Set message UUID to generated value
_EXPORTS_API void
    kvmsg_set_uuid (kvmsg_t *kvmsg);
//  Set message body
_EXPORTS_API void
    kvmsg_del_body (kvmsg_t *kvmsg);
//  Del message body
_EXPORTS_API void
    kvmsg_set_body (kvmsg_t *kvmsg, byte *body, size_t size);
//  Set message key using printf format
_EXPORTS_API void
    kvmsg_fmt_key (kvmsg_t *kvmsg, char *format, ...);
//  Set message body using printf format
_EXPORTS_API void
    kvmsg_fmt_body (kvmsg_t *kvmsg, char *format, ...);

//  Get message property, if set, else ""
_EXPORTS_API char *
    kvmsg_get_prop (kvmsg_t *kvmsg, char *name);
//  Set message property
//  Names cannot contain '='. Max length of value is 255 chars.
_EXPORTS_API void
    kvmsg_set_prop (kvmsg_t *kvmsg, char *name, char *format, ...);

//  Store entire kvmsg into hash map, if key/value are set
//  Nullifies kvmsg reference, and destroys automatically when no longer
//  needed.
_EXPORTS_API void
    kvmsg_store (kvmsg_t **kvmsg_p, zhash_t *hash);
//  Dump message to stderr, for debugging and tracing
_EXPORTS_API void
    kvmsg_dump (kvmsg_t *kvmsg);

//  Runs self test of class
_EXPORTS_API int
    kvmsg_test (int verbose);

#ifdef __cplusplus
}
#endif

#endif      //  Included
