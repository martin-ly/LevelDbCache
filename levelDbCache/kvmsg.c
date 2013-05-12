/*  =====================================================================
*  kvmsg - key-value message class for example applications
*  ===================================================================== */

#include "stdafx.h"
#include "kvmsg.h"
#include "clone.h"
#include "clone_log.h"
//#include <uuid/uuid.h>
#include "zlist.h"

//  Keys are short strings
#define KVMSG_KEY_MAX   255

//  Message is formatted on wire as 4 frames:
//  frame 0: key (0MQ string)
//  frame 1: sequence (8 bytes, network order)
//  frame 2: uuid (blob, 16 bytes)
//  frame 3: properties (0MQ string)
//  frame 4: body (blob)
#define FRAME_KEY       0
#define FRAME_SEQ       1
#define FRAME_UUID      2
#define FRAME_PROPS     3
#define FRAME_BODY      4
#define KVMSG_FRAMES    5

//  Structure of our class
struct _kvmsg {
	//  Presence indicators for each frame
	int present [KVMSG_FRAMES];
	//  Corresponding 0MQ message frames, if any
	zmq_msg_t frame [KVMSG_FRAMES];
	//  Key, copied into safe C string
	char key [KVMSG_KEY_MAX + 1];
	//  List of properties, as name=value strings
	zlist_t *props;
	size_t props_size;
};

//  .split property encoding
//  These two helpers serialize a list of properties to and from a
//  message frame:

static void
	s_encode_props (kvmsg_t *kvmsg)
{
	char *prop ;
	char *dest ; 

	zmq_msg_t *msg = &kvmsg->frame [FRAME_PROPS];
	if (kvmsg->present [FRAME_PROPS])
		zmq_msg_close (msg);

	zmq_msg_init_size (msg, kvmsg->props_size);
	prop = (char* ) zlist_first (kvmsg->props);
	dest = (char *) zmq_msg_data (msg);
	while (prop) {
		strcpy (dest, prop);
		dest += strlen (prop);
		*dest++ = '\n';
		prop = (char *) zlist_next (kvmsg->props);
	}
	kvmsg->present [FRAME_PROPS] = 1;
}

static void
	s_decode_props (kvmsg_t *kvmsg)
{
	size_t remainder;
	char *prop;
	char *eoln;

	zmq_msg_t *msg = &kvmsg->frame [FRAME_PROPS];
	kvmsg->props_size = 0;
	while (zlist_size (kvmsg->props))
		free (zlist_pop (kvmsg->props));

	remainder = zmq_msg_size (msg);
	prop = (char *) zmq_msg_data (msg);
	eoln = (char *) memchr (prop, '\n', remainder);
	while (eoln) {
		*eoln = 0;
		zlist_append (kvmsg->props, strdup (prop));
		kvmsg->props_size += strlen (prop) + 1;
		remainder -= strlen (prop) + 1;
		prop = eoln + 1;
		eoln = (char *) memchr (prop, '\n', remainder);
	}
}

//  .split constructor and destructor
//  Here are the constructor and destructor for the class:

//  Constructor, takes a sequence number for the new kvmsg instance:
kvmsg_t *
	kvmsg_new (int64_t sequence)
{
	kvmsg_t	*kvmsg;

	kvmsg = (kvmsg_t *) zmalloc (sizeof (kvmsg_t));
	kvmsg->props = zlist_new ();
	kvmsg_set_sequence (kvmsg, sequence);
	return kvmsg;
}

//  zhash_free_fn callback helper that does the low level destruction:
void
	kvmsg_free (void *ptr)
{
	if (ptr) {
		
		int frame_nbr;
		kvmsg_t *kvmsg = (kvmsg_t *) ptr;
		//  Destroy message frames if any
		for (frame_nbr = 0; frame_nbr < KVMSG_FRAMES; frame_nbr++)
			if (kvmsg->present [frame_nbr])
				zmq_msg_close (&kvmsg->frame [frame_nbr]);

		//  Destroy property list
		while (zlist_size (kvmsg->props))
			free (zlist_pop (kvmsg->props));
		zlist_destroy (&kvmsg->props);

		//  Free object itself
		free (kvmsg);
	}
}

//  Destructor
void
	kvmsg_destroy (kvmsg_t **kvmsg_p)
{
	assert (kvmsg_p);
	if (*kvmsg_p) {
		kvmsg_free (*kvmsg_p);
		*kvmsg_p = NULL;
	}
}

//  .split recv method
//  The recv method reads a key-value message from socket, and returns a new
//  kvmsg instance:

kvmsg_t *
	kvmsg_recv (void *socket)
{
	kvmsg_t *kvmsg;
	int frame_nbr;

	//  This method is almost unchanged from kvsimple
	//  .skip
	assert (socket);
	kvmsg = kvmsg_new (0);

	//  Read all frames off the wire, reject if bogus
	for (frame_nbr = 0; frame_nbr < KVMSG_FRAMES; frame_nbr++) {
		int rcvmore;
		if (kvmsg->present [frame_nbr])
			zmq_msg_close (&kvmsg->frame [frame_nbr]);
		zmq_msg_init (&kvmsg->frame [frame_nbr]);
		kvmsg->present [frame_nbr] = 1;
		if (zmq_recvmsg (socket, &kvmsg->frame [frame_nbr], 0) == -1) {
			kvmsg_destroy (&kvmsg);
			break;
		}
		//  Verify multipart framing
		rcvmore = (frame_nbr < KVMSG_FRAMES - 1)? 1: 0;
		if (zsockopt_rcvmore (socket) != rcvmore) {
			kvmsg_destroy (&kvmsg);
			break;
		}
	}
	//  .until
	if (kvmsg)
		s_decode_props (kvmsg);
	return kvmsg;
}


//  ---------------------------------------------------------------------
//  Send key-value message to socket; any empty frames are sent as such.

void
	kvmsg_send (kvmsg_t *kvmsg, void *socket)
{
	int frame_nbr;
	assert (kvmsg);
	assert (socket);

	s_encode_props (kvmsg);
	//  The rest of the method is unchanged from kvsimple
	//  .skip
	for (frame_nbr = 0; frame_nbr < KVMSG_FRAMES; frame_nbr++) {
		zmq_msg_t copy;
		zmq_msg_init (&copy);
		if (kvmsg->present [frame_nbr])
			zmq_msg_copy (&copy, &kvmsg->frame [frame_nbr]);
		zmq_sendmsg (socket, &copy,
			(frame_nbr < KVMSG_FRAMES - 1)? ZMQ_SNDMORE: 0);
		zmq_msg_close (&copy);
	}
}
//  .until

//  .split dup method
//  The dup method duplicates a kvmsg instance, returns the new instance:

kvmsg_t *
	kvmsg_dup (kvmsg_t *kvmsg)
{
	char *prop;
	int frame_nbr;
	kvmsg_t *dup = kvmsg_new (0);
	for (frame_nbr = 0; frame_nbr < KVMSG_FRAMES; frame_nbr++) {
		if (kvmsg->present [frame_nbr]) {
			zmq_msg_t *src = &kvmsg->frame [frame_nbr];
			zmq_msg_t *dst = &dup->frame [frame_nbr];
			zmq_msg_init_size (dst, zmq_msg_size (src));
			memcpy (zmq_msg_data (dst),
				zmq_msg_data (src), zmq_msg_size (src));
			dup->present [frame_nbr] = 1;
		}
	}
	dup->props_size = zlist_size (kvmsg->props);
	prop = (char *) zlist_first (kvmsg->props);
	while (prop) {
		zlist_append (dup->props, strdup (prop));
		prop = (char *) zlist_next (kvmsg->props);
	}
	return dup;
}

//  The key, sequence, body, and size methods are the same as in kvsimple.
//  .skip

//  ---------------------------------------------------------------------
//  Return key from last read message, if any, else NULL

char *
	kvmsg_key (kvmsg_t *kvmsg)
{
	assert (kvmsg);
	if (kvmsg->present [FRAME_KEY]) {
		if (!*kvmsg->key) {
			size_t size = zmq_msg_size (&kvmsg->frame [FRAME_KEY]);
			if (size > KVMSG_KEY_MAX)
				size = KVMSG_KEY_MAX;
			memcpy (kvmsg->key, zmq_msg_data (&kvmsg->frame [FRAME_KEY]), size);
			kvmsg->key [size] = 0;
		}
		return kvmsg->key;
	}
	else
		return NULL;
}


//  ---------------------------------------------------------------------
//  Set message key as provided

void
	kvmsg_set_key (kvmsg_t *kvmsg, char *key)
{
	zmq_msg_t *msg;

	assert (kvmsg);
	msg = &kvmsg->frame [FRAME_KEY];
	if (kvmsg->present [FRAME_KEY])
		zmq_msg_close (msg);
	zmq_msg_init_size (msg, strlen (key));
	memcpy (zmq_msg_data (msg), key, strlen (key));
	kvmsg->present [FRAME_KEY] = 1;
}


//  ---------------------------------------------------------------------
//  Set message key using printf format

void
	kvmsg_fmt_key (kvmsg_t *kvmsg, char *format, ...)
{
	char value [KVMSG_KEY_MAX + 1];
	va_list args;

	assert (kvmsg);
	va_start (args, format);
	vsnprintf (value, KVMSG_KEY_MAX, format, args);
	va_end (args);
	kvmsg_set_key (kvmsg, value);
}


//  ---------------------------------------------------------------------
//  Return sequence nbr from last read message, if any

int64_t
	kvmsg_sequence (kvmsg_t *kvmsg)
{
	byte *source;
	int64_t sequence;

	assert (kvmsg);
	if (kvmsg->present [FRAME_SEQ]) {
		assert (zmq_msg_size (&kvmsg->frame [FRAME_SEQ]) == 8);
		source = (byte *) zmq_msg_data (&kvmsg->frame [FRAME_SEQ]);
		sequence = ((int64_t) (source [0]) << 56)
			+ ((int64_t) (source [1]) << 48)
			+ ((int64_t) (source [2]) << 40)
			+ ((int64_t) (source [3]) << 32)
			+ ((int64_t) (source [4]) << 24)
			+ ((int64_t) (source [5]) << 16)
			+ ((int64_t) (source [6]) << 8)
			+  (int64_t) (source [7]);
		return sequence;
	}
	else
		return 0;
}


//  ---------------------------------------------------------------------
//  Set message sequence number

void
	kvmsg_set_sequence (kvmsg_t *kvmsg, int64_t sequence)
{
	zmq_msg_t *msg;
	byte *source;

	assert (kvmsg);
	msg = &kvmsg->frame [FRAME_SEQ];
	if (kvmsg->present [FRAME_SEQ])
		zmq_msg_close (msg);
	zmq_msg_init_size (msg, 8);

	source = (byte *) zmq_msg_data (msg);
	source [0] = (byte) ((sequence >> 56) & 255);
	source [1] = (byte) ((sequence >> 48) & 255);
	source [2] = (byte) ((sequence >> 40) & 255);
	source [3] = (byte) ((sequence >> 32) & 255);
	source [4] = (byte) ((sequence >> 24) & 255);
	source [5] = (byte) ((sequence >> 16) & 255);
	source [6] = (byte) ((sequence >> 8)  & 255);
	source [7] = (byte) ((sequence)       & 255);

	kvmsg->present [FRAME_SEQ] = 1;
}

//  ---------------------------------------------------------------------
//  Return body from last read message, if any, else NULL

byte *
	kvmsg_body (kvmsg_t *kvmsg)
{
	assert (kvmsg);
	if (kvmsg->present [FRAME_BODY])
		return (byte *) zmq_msg_data (&kvmsg->frame [FRAME_BODY]);
	else
		return NULL;
}


//  ---------------------------------------------------------------------
//  Set message body

void
	kvmsg_set_body (kvmsg_t *kvmsg, byte *body, size_t size)
{
	zmq_msg_t *msg;

	assert (kvmsg);
	msg = &kvmsg->frame [FRAME_BODY];
	if (kvmsg->present [FRAME_BODY])
		zmq_msg_close (msg);
	kvmsg->present [FRAME_BODY] = 1;
	zmq_msg_init_size (msg, size);
	memcpy (zmq_msg_data (msg), body, size);
}

//  ---------------------------------------------------------------------
//  Del message body

void
	kvmsg_del_body (kvmsg_t *kvmsg)
{
	zmq_msg_t *msg;

	assert (kvmsg);
	msg = &kvmsg->frame [FRAME_BODY];
	if (kvmsg->present [FRAME_BODY])
		zmq_msg_close (msg);
	kvmsg->present [FRAME_BODY] = 0;
	zmq_msg_init_size (msg, 0);
	memcpy (zmq_msg_data (msg), (byte *) "", 0);
}

//  ---------------------------------------------------------------------
//  Set message body using printf format

void 
	kvmsg_fmt_body (kvmsg_t *kvmsg, char *format, ...)
{
	va_list args;
	int len;
	char * buffer;

	assert (kvmsg);
	va_start(args, format);
	// _vscprintf doesn't count terminating '\0'
	len = _vscprintf(format, args) + 1;
	buffer = (char*) malloc( len * sizeof(char) );
	vsprintf_s( buffer, len, format, args );
	kvmsg_set_body (kvmsg, (byte *)buffer, len);
	va_end (args);
	free(buffer);
}


//  ---------------------------------------------------------------------
//  Return body size from last read message, if any, else zero

size_t 
	kvmsg_size (kvmsg_t *kvmsg)
{
	assert (kvmsg);
	if (kvmsg->present [FRAME_BODY])
		return zmq_msg_size (&kvmsg->frame [FRAME_BODY]);
	else
		return 0;
}
//  .until

//  .split UUID methods
//  These methods get/set the UUID for the key-value message:

byte *
	kvmsg_uuid (kvmsg_t *kvmsg)
{
	assert (kvmsg);
	//clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "UC kvmsg_uuid sizeof uuid=%d", zmq_msg_size (&kvmsg->frame [FRAME_UUID]));*/
	if (kvmsg->present [FRAME_UUID] &&  zmq_msg_size (&kvmsg->frame [FRAME_UUID]) == 16) {
		//clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "UC kvmsg_uuid NOT NULL");
		return (byte *) zmq_msg_data (&kvmsg->frame [FRAME_UUID]);
	} else {
		//clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "UC kvmsg_uuid NULL");
		return NULL;
	}
}

//  Sets the UUID to a random generated value
void
	kvmsg_set_uuid (kvmsg_t *kvmsg)
{
	zmq_msg_t *msg;     
	int randm_uuid_high, randm_uuid_ligh;     // pour remplacer le uuid et uuid-generte() on va générer un chiffre aléatoire avec la fonction randof(100000000) un nombre de neuf chiffre
	char uniqueid[16] ; // Cet atribut fait appelle à la librairie <uuid.h> qui ne peut pas être compilé sous windows 
					 // La fonction uuid_generate (uuid) va générere un ID unique en aléatoire en 16 octets pour le passer au paramètre uuid

	assert (kvmsg);
	msg = &kvmsg->frame [FRAME_UUID];

	//uuid_generate (uuid);
	randm_uuid_high= randof(10000000) ;
	randm_uuid_ligh= randof(10000000) ; 
	sprintf(uniqueid, "%i%i", randm_uuid_high, randm_uuid_ligh) ;  // converion de randm_uuid en char

	if (kvmsg->present [FRAME_UUID])
		zmq_msg_close (msg);
	zmq_msg_init_size (msg, sizeof (uniqueid));
	memcpy (zmq_msg_data (msg), uniqueid, sizeof (uniqueid));
	kvmsg->present [FRAME_UUID] = 1;
}

//  .split property methods
//  These methods get/set a specified message property:

//  Get message property, return "" if no such property is defined.
char *
	kvmsg_get_prop (kvmsg_t *kvmsg, char *name)
{
	char *prop ;
	size_t namelen;

	assert (strchr (name, '=') == NULL);
	prop = (char *) zlist_first (kvmsg->props);
	namelen = strlen (name);
	while (prop) {
		if (strlen (prop) > namelen
			&&  memcmp (prop, name, namelen) == 0
			&&  prop [namelen] == '=')
			return prop + namelen + 1;
		prop = (char *) zlist_next (kvmsg->props);
	}
	return "" ;
}


//  Set message property. Property name cannot contain '='. Max length of
//  value is 255 chars.
void
	kvmsg_set_prop (kvmsg_t *kvmsg, char *name, char *format, ...)
{
	char *value;
	va_list args;
	char *prop ;
	char *existing ;	
	int len;

	assert (strchr (name, '=') == NULL);
	assert (kvmsg);

	va_start(args, format);
	// _vscprintf doesn't count terminating '\0'
	len = _vscprintf(format, args) + 1;
	value = (char*) malloc( len * sizeof(char) );
	vsprintf_s( value, len, format, args );
	va_end (args);
	
	//  Allocate name=value string
	prop = (char *) malloc (strlen (name) + strlen (value) + 2);

	//  Remove existing property if any
	sprintf (prop, "%s=", name);
	existing = (char *) zlist_first (kvmsg->props);
	while (existing) {
		if (memcmp (prop, existing, strlen (prop)) == 0) {
			kvmsg->props_size -= strlen (existing) + 1;
			zlist_remove (kvmsg->props, existing);
			free (existing);
			break;
		}
		existing = (char *) zlist_next (kvmsg->props);
	}
	//  Add new name=value property string
	strcat (prop, value);
	zlist_append (kvmsg->props, prop);
	kvmsg->props_size += strlen (prop) + 1;
	free(value);
}

//  .split store method
//  The store method stores the key-value message into a hash map, unless
//  the key and value are both null. It nullifies the kvmsg reference so
//  that the object is owned by the hash map, not the caller:

void
	kvmsg_store (kvmsg_t **kvmsg_p, zhash_t *hash)
{
	assert (kvmsg_p);
	if (*kvmsg_p) {
		kvmsg_t *kvmsg = *kvmsg_p;
		assert (kvmsg);
		if (kvmsg->present [FRAME_BODY] && kvmsg_size (kvmsg)) {
			if (kvmsg->present [FRAME_KEY]) {
				zhash_update (hash, kvmsg_key (kvmsg), kvmsg);
				zhash_freefn (hash, kvmsg_key (kvmsg), kvmsg_free);
			}
		}
		else
			zhash_delete (hash, kvmsg_key (kvmsg));

		*kvmsg_p = NULL;
	}
}

//  .split dump method
//  The dump method extends the kvsimple implementation with support for
//  message properties:

void
	kvmsg_dump (kvmsg_t *kvmsg)
{
	size_t size;
	byte  *body;
	char *prop;

	//  .skip
	if (kvmsg) {
		int char_nbr;

		if (!kvmsg) {
			zclock_log("NULL");
			return;
		}
		size = kvmsg_size (kvmsg);
		body = kvmsg_body (kvmsg);
		zclock_log("[seq:%I64d]", kvmsg_sequence (kvmsg));
		zclock_log("[key:%s]", kvmsg_key (kvmsg));
		//  .until
		zclock_log("[size:%zd] ", size);
		if (zlist_size (kvmsg->props)) {
			zclock_log("[");
			prop = (char *) zlist_first (kvmsg->props);
			while (prop) {
				zclock_log("%s;", prop);
				prop = (char *) zlist_next (kvmsg->props);
			}
			zclock_log("]");
		}
		//  .skip
		for (char_nbr = 0; char_nbr < size; char_nbr++)
			zclock_log("%02X", body [char_nbr]);
		zclock_log("\n");
	}
	else
		zclock_log("NULL message\n");
}
//  .until

//  .split test method
//  The selftest method is the same as in kvsimple with added support
//  for the uuid and property features of kvmsg:

int
	kvmsg_test (int verbose)
{
	//  .skip
	kvmsg_t	*kvmsg;
	zctx_t *ctx;
	void *output;
	int rc;
	void *input;
	zhash_t *kvmap;

	printf (" * kvmsg: ");

	//  Prepare our context and sockets
	ctx = zctx_new ();
	output = zsocket_new (ctx, ZMQ_DEALER);
	rc = zmq_bind (output, "ipc://kvmsg_selftest.ipc");
	assert (rc == 0);
	input = zsocket_new (ctx, ZMQ_DEALER);
	rc = zmq_connect (input, "ipc://kvmsg_selftest.ipc");
	assert (rc == 0);

	kvmap = zhash_new ();

	//  .until
	//  Test send and receive of simple message
	kvmsg = kvmsg_new (1);
	kvmsg_set_key  (kvmsg, "key");
	kvmsg_set_uuid (kvmsg);
	kvmsg_set_body (kvmsg, (byte *) "body", 4);
	if (verbose)
		kvmsg_dump (kvmsg);
	kvmsg_send (kvmsg, output);
	kvmsg_store (&kvmsg, kvmap);

	kvmsg = kvmsg_recv (input);
	if (verbose)
		kvmsg_dump (kvmsg);
	assert (streq (kvmsg_key (kvmsg), "key"));
	kvmsg_store (&kvmsg, kvmap);

	//  Test send and receive of message with properties
	kvmsg = kvmsg_new (2);
	kvmsg_set_prop (kvmsg, "prop1", "value1");
	kvmsg_set_prop (kvmsg, "prop2", "value1");
	kvmsg_set_prop (kvmsg, "prop2", "value2");
	kvmsg_set_key  (kvmsg, "key");
	kvmsg_set_uuid (kvmsg);
	kvmsg_set_body (kvmsg, (byte *) "body", 4);
	assert (streq (kvmsg_get_prop (kvmsg, "prop2"), "value2"));
	if (verbose)
		kvmsg_dump (kvmsg);
	kvmsg_send (kvmsg, output);
	kvmsg_destroy (&kvmsg);

	kvmsg = kvmsg_recv (input);
	if (verbose)
		kvmsg_dump (kvmsg);
	assert (streq (kvmsg_key (kvmsg), "key"));
	assert (streq (kvmsg_get_prop (kvmsg, "prop2"), "value2"));
	kvmsg_destroy (&kvmsg);
	//  .skip
	//  Shutdown and destroy all objects
	zhash_destroy (&kvmap);
	zctx_destroy (&ctx);

	printf ("OK\n");
	return 0;
}
//  .until
