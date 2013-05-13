/*  =====================================================================
  clone - server-side Clone Pattern class

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


//  Lets us build this source without creating a library
#include "stdafx.h"
#include "bstar.h"
#include "kvmsg.h"
#include "clone.h"
#include "clone_log.h"

//  .split definitions
//  We define a set of reactor handlers and our server object structure:

//  Bstar reactor handlers
static int send_snapshot  (zloop_t *loop, zmq_pollitem_t *poller, void *args);
static int s_collector  (zloop_t *loop, zmq_pollitem_t *poller, void *args);
static int s_flush_ttl  (zloop_t *loop, zmq_pollitem_t *poller, void *args);
static int s_send_hugz  (zloop_t *loop, zmq_pollitem_t *poller, void *args);
static int s_new_active (zloop_t *loop, zmq_pollitem_t *poller, void *args);
static int s_new_passive  (zloop_t *loop, zmq_pollitem_t *poller, void *args);
static int s_subscriber (zloop_t *loop, zmq_pollitem_t *poller, void *args);

//  Routing information for a key-value snapshot
typedef struct {
	void *socket;           //  ROUTER socket to send to
	zframe_t *identity;     //  Identity of peer who requested state
	char *subtree;          //  Client subtree specification
} kvroute_t;

static memcache_t *
	memcache_new (base_t *base, int cacheid, char *dbPath)
{
	extern struct clone_parameters *params;
	char* errptr ;
	clonesrv_t *clonesrv = (clonesrv_t *) base->clonesrv ;
	memcache_t *memcache = (memcache_t *) zmalloc (sizeof (memcache_t));
	base_parameters *base_params = params->bases[base->baseid];
	memcache->base = base;
	memcache->dbPath= dbPath;
	memcache->dbOptions = leveldb_options_create();
	leveldb_options_set_create_if_missing(memcache->dbOptions, 'true' );
	leveldb_options_set_compression(memcache->dbOptions, 0) ;
	strncpy (memcache->cacheidstr, base_params->cacheids[cacheid], MAXLEN);
	//Pour backup les kvmap sont cree lors de la reception des snapshots
	if (clonesrv->primary)
		memcache->kvmap = zhash_new ();
	memcache->pending = zlist_new ();
	memcache->db = leveldb_open( memcache->dbOptions, memcache->dbPath , &errptr) ;
	memcache->dbPath = dbPath;
	return memcache;
}

static void
	memcache_destroy (memcache_t **memcache_p)
{
	assert (memcache_p);
	if (*memcache_p) {
		memcache_t *memcache = *memcache_p;
		while (zlist_size (memcache->pending)) {
			kvmsg_t *kvmsg = (kvmsg_t *) zlist_pop (memcache->pending);
			kvmsg_destroy (&kvmsg);
		}
		zlist_destroy (&memcache->pending);
		zhash_destroy (&memcache->kvmap);
		free (memcache);
		*memcache_p = NULL;
	}
}

static void
	base_addcache (base_t *base, char *cacheidstr, char *dbPath)
{
	clonesrv_t *clonesrv;
	assert (base);
	clonesrv = (clonesrv_t *)base->clonesrv;
	strcpy(base->cacheids[base->nbr_memcaches], cacheidstr);
	base->memcaches [base->nbr_memcaches] = memcache_new (base, base->nbr_memcaches, dbPath);
	clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: base_addcache cacheid=%d", base->nbr_memcaches);
	zloop_timer  (bstar_zloop (clonesrv->bstar), 1000, 0, s_send_hugz, base->memcaches [base->nbr_memcaches]);
	base->nbr_memcaches++;
}

static base_t *
	base_new (clonesrv_t *clonesrv, int baseid)
{
	extern struct clone_parameters *params;
	zmq_pollitem_t poller;
	char* errptr ;
	int cacheid;
	base_parameters *base_params = params->bases[baseid];
	char *baseidstr = base_params->baseidstr;
	base_t *base = (base_t *) zmalloc (sizeof (base_t));
	//  Initialize the Binary Star
	base->ctx = zctx_new ();
	base->clonesrv = clonesrv;
	base->port = base_params->port;
	base->peer = base_params->peer;
	bstar_snapshot_req_receptor (clonesrv->bstar, base_params->bstarReceptor, ZMQ_ROUTER, send_snapshot, base);
	//  Set up our clone server sockets
	base->publisher = zsocket_new (base->ctx, ZMQ_PUB);
	base->collector = zsocket_new (base->ctx, ZMQ_SUB);
	zsockopt_set_subscribe (base->collector, "");
	zsocket_bind (base->publisher, "tcp://*:%d", base->port + 1);
	zsocket_bind (base->collector, "tcp://*:%d", base->port + 2);
	//  Set up our own clone client interface to peer
	base->subscriber = zsocket_new (base->ctx, ZMQ_SUB);
	zsockopt_set_subscribe (base->subscriber, "");
	zsocket_connect (base->subscriber, "tcp://localhost:%d", base->peer + 1);
	//  .split main task body
	//  After we've set-up our sockets we register our binary star
	//  event handlers, and then start the bstar reactor. This finishes
	//  when the user presses Ctrl-C, or the process receives a SIGINT
	//  interrupt:
	//  Register our other handlers with the bstar reactor
	// zmq_pollitem_t poller = { clonesrv->collector, 0, ZMQ_POLLIN };
	poller.socket=base->collector ;
	poller.fd=0 ;
	poller.events = ZMQ_POLLIN ;

	zloop_poller (bstar_zloop (clonesrv->bstar), &poller, s_collector, base);
	//TODO FOR EACH BASE OR EACH CACHE ?? zloop_timer  (bstar_zloop (clonesrv->bstar), 1000, 0, s_send_hugz, clonesrv->bases[baseid]);
	strncpy (base->baseidstr, baseidstr, MAXLEN);
	base->nbr_memcaches = 0;
	for (cacheid = 0; cacheid < base_params->nbr_memcaches ; cacheid++) {
		base_addcache (base, base_params->cacheids[cacheid]  , base_params->databasePath);
	}
	return base;
}

static void
	base_destroy (base_t **base_p)
{
	int cacheid;
	assert (base_p);
	if (*base_p) {
		base_t *base = *base_p;
		for (cacheid = 0; cacheid < base->nbr_memcaches; cacheid) {
			memcache_t *memcache = base->memcaches[cacheid];
			memcache_destroy (&memcache);
		}
		zctx_destroy (&base->ctx);
		free (base);
		*base_p = NULL;
	}
}

static void
	clonesrv_addbase (clonesrv_t *clonesrv, int baseid)
{
	assert (clonesrv);
	clonesrv->bases [clonesrv->nbr_bases] = base_new (clonesrv, baseid);
	clonesrv->nbr_bases++;
}

static base_t *
	clonesrv_getbase (clonesrv_t *clonesrv, char *baseidstr)
{
	int baseid;
	assert(clonesrv);
	for (baseid = 0; baseid < clonesrv->nbr_bases; baseid++) {
		if (streq (baseidstr, clonesrv->baseids[baseid])) {
			return clonesrv->bases[baseid];
		}
	}
	return NULL;
}

static int
	clonesrv_getbaseid (clonesrv_t *clonesrv, char *baseidstr)
{
	int baseid;
	assert (clonesrv);
	for (baseid = 0; baseid < clonesrv->nbr_bases; baseid++)
		if (streq (baseidstr, clonesrv->baseids[baseid]))
			return baseid;
	return -1;
}

static memcache_t *
	base_getcache (base_t *base, char *cacheidstr)
{
	int cacheid;
	assert(base);
	for (cacheid = 0; cacheid < base->nbr_memcaches; cacheid++) {
		if (streq (cacheidstr, base->cacheids[cacheid])) {
			return base->memcaches [cacheid];
		}
	}
	return NULL;
}

static int
	base_getcacheid (base_t *base, char *cacheidstr)
{
	int cacheid;
	assert (base);
	for (cacheid = 0; cacheid < base->nbr_memcaches; cacheid++)
		if (streq (cacheidstr, base->cacheids[cacheid]))
			return cacheid;
	return -1;
}

//  .split main task setup
//  The main task parses the command line to decide whether to start
//  as primary or backup server. We're using the Binary Star pattern
//  for reliability. This interconnects the two servers so they can
//  agree on which is primary, and which is backup. To allow the two
//  servers to run on the same box, we use different ports for primary
//  and backup. Ports 5003/5004 are used to interconnect the servers.
//  Ports 5556/5566 are used to receive voting events (snapshot requests
//  in the clone pattern). Ports 5557/5567 are used by the publisher,
//  and ports 5558/5568 by the collector:


void launchServer (int argc, char* confPath)
{
	int baseid;
	clonesrv_t *clonesrv = (clonesrv_t *) zmalloc (sizeof (clonesrv_t));

	if (argc == 1 || argc == 2){
		bstar_t *bstar;
		leveldb_options_t *options ;
		zclock_log ("I: launchServer Initializing parameters to default values...");
		init_parameters ();
		parse_config(confPath);
		//  Register our logs handlers
		clone_log_new();
		clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: launchServer : %d, ServerType=%s, bstarLocal=%s bstarRemote=%s", params->primary, params->ServerType, params->bstarLocal, params->bstarRemote);

		clonesrv->ClusterName=params->ClusterName;
		clonesrv->primary = params->primary;
		clonesrv->ServerType= params->ServerType;
		if (clonesrv->primary) {
			clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: launchServer primary active, waiting for backup (passive)");
			bstar = bstar_new (BSTAR_PRIMARY, params->bstarLocal, params->bstarRemote);
		}
		else {
			clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: launchServer backup passive, waiting for primary (active)");
			bstar = bstar_new (BSTAR_BACKUP, params->bstarLocal, params->bstarRemote);
		}
		clonesrv->bstar=bstar;
		//Create memcaches for primary, for backup they are created when receiving snapshots
		clonesrv->nbr_bases = 0;
		for (baseid = 0; baseid < params->nbr_bases; baseid++) {
			clonesrv_addbase (clonesrv, baseid);
		}
	}
	else {
		zclock_log ("I: Usage: clonesrv { -p | -b }\n");
		free (clonesrv);
		exit (0);
	}
	bstar_set_verbose (clonesrv, FALSE);
	//  Register state change handlers
	bstar_new_active (clonesrv->bstar, s_new_active, clonesrv);
	bstar_new_passive (clonesrv->bstar, s_new_passive, clonesrv);
	//  Start the Bstar reactor
	bstar_start (clonesrv->bstar);

	//  Interrupted, so shut down
	zclock_log ("I: shut down");
	for (baseid = 0; baseid < clonesrv->nbr_bases; baseid++)
		base_destroy (&clonesrv->bases [baseid]);
	bstar_destroy (&clonesrv->bstar);
	zctx_destroy (&clonesrv->ctx);
	clone_log_destroy();
	free (clonesrv);
}

//  We handle GETSNAPSHOT requests exactly as in the clonesrv5 example.
//  .skip


//  Send one state snapshot key-value pair to a socket
//  Hash item data is our kvmsg object, ready to send
static int
	s_send_single (const char *key, void *data, void *args)
{
	kvroute_t *kvroute = (kvroute_t *) args;
	kvmsg_t *kvmsg = (kvmsg_t *) data;
	//clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: s_send_single [seq:%I64d] [key:%s]", kvmsg_sequence (kvmsg), kvmsg_key (kvmsg));
	if (strlen (kvroute->subtree) <= strlen (kvmsg_key (kvmsg)) &&  memcmp (kvroute->subtree, kvmsg_key (kvmsg), strlen (kvroute->subtree)) == 0) {
		zframe_send (&kvroute->identity,    //  Choose recipient
			kvroute->socket, ZFRAME_MORE + ZFRAME_REUSE);
		kvmsg_send (kvmsg, kvroute->socket);
	}
	return 0;
}

static int
	send_snapshot (zloop_t *loop, zmq_pollitem_t *poller, void *args)
{
	int cacheid;
	kvmsg_t *kvmsg;
	memcache_t *memcache = NULL;
	base_t *base = (base_t *) args;
	int64_t  sequence;

	zframe_t *identity = zframe_recv (poller->socket);
	if (identity) {
		//  Request is in second frame of message
		char *cacheidstr;
		char *subtree;
		char *request = zstr_recv (poller->socket);
		subtree="";
		if (streq (request, "GETSNAPSHOT")) {
			clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: send_snapshot receiving request GETSNAPSHOT base=%d", base->baseid );
			free (request);
		}
		else
			clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "E: send_snapshot bad request, aborting\n");

		for (cacheid = 0; cacheid < base->nbr_memcaches; cacheid++) {
			memcache_t *memcache = base->memcaches[cacheid];

			if (memcache->kvmap) {
				kvmsg_t *kvmsg;
				//  Send state socket to client
				kvroute_t routing = { poller->socket, identity, subtree };
				sequence = memcache->sequence;

				//  Send snapshot enreg to client
				clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: sending SNAPSHOT");
				zframe_send (&identity, poller->socket, ZFRAME_MORE + ZFRAME_REUSE);
				kvmsg = kvmsg_new (memcache->sequence);
				kvmsg_set_key  (kvmsg, "BEGINMEMCACHE");
				kvmsg_set_prop (kvmsg, "cacheidstr", "%s", memcache->cacheidstr);
				kvmsg_set_body (kvmsg, (byte *) subtree, 0);
				kvmsg_send     (kvmsg, poller->socket);
				kvmsg_destroy (&kvmsg);
				//zframe_send (&identity, poller->socket, ZFRAME_MORE + ZFRAME_REUSE);
				//Envoie des elements du hashmap
				zhash_foreach (memcache->kvmap, s_send_single, &routing);
				//  Now send END message with sequence number
				clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: sent end snapshots MEMCACHE");
				sequence = memcache->sequence;
			} else {
				clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: send_snapshot receiving GETSNAPSHOT base=%s cacheid=%s NO KVMAP", base->baseidstr, cacheidstr, memcache->dbPath );
			}
		}
		clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: sending end snapshots ENDSNAPSHOT");
		zframe_send (&identity, poller->socket, ZFRAME_MORE);
		kvmsg = kvmsg_new (sequence);
		kvmsg_set_key  (kvmsg, "ENDSNAPSHOT");
		kvmsg_set_body (kvmsg, (byte *) subtree, 0);
		kvmsg_send     (kvmsg, poller->socket);
		kvmsg_destroy (&kvmsg);
		zframe_destroy(&identity);
	}
	return 0;
}
//  .until

//  .split collect updates
//  The collector is more complex than in the clonesrv5 example since how
//  process updates depends on whether we're active or passive. The active
//  applies them immediately to its kvmap, whereas the passive queues them
//  as pending:

//  If message was already on pending list, remove it and return TRUE,
//  else return FALSE.
static int
	s_was_pending (memcache_t *memcache, kvmsg_t *kvmsg)
{
	int64_t ttld;
	kvmsg_t *held;
	sscanf (kvmsg_get_prop (kvmsg, "ttld"), "%I64d", &ttld);
	//clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "UC s_was_pending deleted by ttl don't set in pending list");
	if (ttld)
		return TRUE;
	held = (kvmsg_t *) zlist_first (memcache->pending);
	while (held) {
		if (memcmp (kvmsg_uuid (held), kvmsg_uuid (kvmsg), 16) == 0){
			zlist_remove (memcache->pending, held);
			return TRUE;
		}
		held = (kvmsg_t *) zlist_next (memcache->pending);
	}
	return FALSE;
}

static int
	s_collector (zloop_t *loop, zmq_pollitem_t *poller, void *args)
{
	char* errptr ;
	memcache_t *memcache = NULL;
	leveldb_writeoptions_t *write_options;
	base_t *base = (base_t *) args;
	clonesrv_t *clonesrv = (clonesrv_t *)base->clonesrv;
	kvmsg_t *kvmsg = kvmsg_recv (poller->socket);
	if (kvmsg) {
		char *cacheidstr = kvmsg_get_prop (kvmsg, "cacheidstr");
		if (strneq (cacheidstr, "")) {
			memcache = base_getcache (base, cacheidstr);
		}
		if (clonesrv->active) {
			int64_t ttl;
			char *key;
			char *body;
			int size;
			char SNumber[14];

			kvmsg_set_sequence (kvmsg, ++memcache->sequence);
			sscanf (kvmsg_get_prop (kvmsg, "ttl"), "%I64d", &ttl);
			if (ttl)
			{
				kvmsg_set_prop (kvmsg, "ttl", "%I64d", zclock_time () + ttl * 1000);
			}
			kvmsg_send (kvmsg, base->publisher);

			key= kvmsg_key(kvmsg) ;
			size = kvmsg_size(kvmsg) ;
			if (size) {
				body= (char*) kvmsg_body(kvmsg) ;
			}
			else {
				body = "";
			}

			kvmsg_store (&kvmsg, memcache->kvmap);

			//////db Persistence
			write_options = leveldb_writeoptions_create() ;
			leveldb_put( memcache->db, write_options, key, strlen(key)+1 , body, strlen(body)+1, &errptr) ;
			sprintf_s(SNumber, 14,"%I64d", (int64_t) memcache->sequence) ;
			leveldb_put( memcache->db, write_options, "SEQUENCENUMBER", 15, SNumber, strlen(SNumber)+1, &errptr) ;
			//////db Persistence
		}
		else {
			//Passive If we already got message from active, drop it, else hold on pending list
			if (s_was_pending (memcache, kvmsg))
				kvmsg_destroy (&kvmsg);
			else
				zlist_append (memcache->pending, kvmsg);
		}
	}
	return 0;
}

//  We purge ephemeral values using exactly the same code as in
//  the previous clonesrv5 example.
//  .skip
//  If key-value pair has expired, delete it and publish the
//  fact to listening clients.
static int
	s_flush_single (const char *key, void *data, void *args)
{
	int64_t ttl;
	memcache_t *memcache = (memcache_t *) args;
	base_t *base = (base_t *) memcache->base;
	kvmsg_t *kvmsg = (kvmsg_t *) data;
	sscanf (kvmsg_get_prop (kvmsg, "ttl"), "%I64d", &ttl);
	//clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: DUMP s_flush_single publishing ttlStr=%s", ttlStr);
	if (ttl && (zclock_time () >= ttl) ) {
		kvmsg_set_sequence (kvmsg, ++memcache->sequence);
		//Pour ne pas mettre en pendinglist
		kvmsg_set_prop (kvmsg, "ttld", "%d", 1);
		kvmsg_del_body (kvmsg);
		kvmsg_send     (kvmsg, base->publisher);
		kvmsg_store (&kvmsg, memcache->kvmap);
	}
	return 0;
}

static int
	s_flush_ttl (zloop_t *loop, zmq_pollitem_t *poller, void *args)
{
	uint cacheid;
	base_t *base = (base_t *) args;
	for (cacheid = 0; cacheid < base->nbr_memcaches; cacheid++)
	{
		memcache_t *memcache = base->memcaches [cacheid];
		if (memcache->kvmap)
			zhash_foreach (memcache->kvmap, s_flush_single, memcache);
	}
	return 0;
}

//  .until

//  .split heartbeating
//  We send a HUGZ message once a second to all subscribers so that they
//  can detect if our server dies. They'll then switch over to the backup
//  server, which will become active:

static int
	s_send_hugz (zloop_t *loop, zmq_pollitem_t *poller, void *args)
{
	kvmsg_t *kvmsg;
	memcache_t *memcache = (memcache_t *) args;
	base_t *base = (base_t *) memcache->base;

	kvmsg = kvmsg_new (memcache->sequence);
	kvmsg_set_key  (kvmsg, "HUGZ");
	kvmsg_set_prop (kvmsg, "cacheidstr", memcache->cacheidstr);
	kvmsg_set_body (kvmsg, (byte *) "", 0);
	kvmsg_send     (kvmsg, base->publisher);
	kvmsg_destroy (&kvmsg);

	return 0;
}

//  .split handling state changes
//  When we switch from passive to active, we apply our pending list so that
//  our kvmap is up-to-date. When we switch to passive, we wipe our kvmap
//  and grab a new snapshot from the active:

static int
	s_new_active (zloop_t *loop, zmq_pollitem_t *unused, void *args)
{
	//////db Persistence
	char* SN;
	char* errptr ;
	size_t vallenth ;
	leveldb_readoptions_t *read_options ;
	int baseid;
	int cacheid;
	//////db Persistence

	zmq_pollitem_t poller;// = { 0, 0, 0 };
	clonesrv_t *clonesrv = (clonesrv_t *) args;
	clonesrv->active = TRUE;
	clonesrv->passive = FALSE;
	clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "ClusterName %s (%s) s_new_active", clonesrv->ClusterName, clonesrv->ServerType)  ;

	for (baseid = 0; baseid < clonesrv->nbr_bases; baseid++) {
		base_t *base = clonesrv->bases[baseid];
		//  Stop subscribing to updates
		//zmq_pollitem_t poller = { clonesrv->subscriber, 0, ZMQ_POLLIN };
		poller.socket = base->subscriber;
		poller.fd=0 ;
		poller.events = ZMQ_POLLIN;
		zloop_poller_end (bstar_zloop (clonesrv->bstar), &poller);

		//seulement au premier démarage en tant que active

		for (cacheid = 0; cacheid < base->nbr_memcaches; cacheid++)
		{
			memcache_t *memcache = base->memcaches [cacheid];
			if(memcache->sequence==0){
				read_options = leveldb_readoptions_create() ;
				vallenth=0 ;
				//Sequence Number from database
				SN=leveldb_get( memcache->db, read_options, "SEQUENCENUMBER", 15, &vallenth, &errptr) ;

				//Hashmap Recovery
				if(SN){
					leveldb_iterator_t *iterator ;
					leveldb_writeoptions_t *write_options;
					// continue from this sequence number

					sscanf (SN, "%I64d", &memcache->sequence);
					//Fill Hashmap
					iterator=leveldb_create_iterator( memcache->db, read_options);
					for (leveldb_iter_seek_to_first(iterator); leveldb_iter_valid(iterator); leveldb_iter_next(iterator)) {
						kvmsg_t *kvmsg ;
						size_t sizeKey, sizevalue;
						char *key;
						char *value;
						key = (char *)leveldb_iter_key( iterator, &sizeKey);
						value = (char *)leveldb_iter_value( iterator, &sizevalue);	
						if(strneq (key, "SEQUENCENUMBER")){
							kvmsg = kvmsg_new (0);
							kvmsg_set_key  (kvmsg, key);
							kvmsg_set_uuid (kvmsg);
							kvmsg_fmt_body (kvmsg, "%s", value);
							kvmsg_store (&kvmsg, memcache->kvmap);
						}
					}
				}
			}

			//  Apply pending list to own hash table
			while (zlist_size (memcache->pending)) {
				char *key;
				char *body;
				int size;
				char SNumber[14];
				leveldb_writeoptions_t *write_options;
				kvmsg_t *kvmsg = (kvmsg_t *) zlist_pop (memcache->pending);
				kvmsg_set_sequence (kvmsg, ++memcache->sequence);
				kvmsg_send (kvmsg, base->publisher);
				key= kvmsg_key(kvmsg) ;
				size = kvmsg_size(kvmsg) ;
				if (size) {
					body= (char*) kvmsg_body(kvmsg) ;
				}
				else {
					body = "";
				}
				kvmsg_store (&kvmsg, memcache->kvmap);
				write_options = leveldb_writeoptions_create() ;
				leveldb_put( memcache->db, write_options, key, strlen(key)+1 , body, strlen(body)+1, &errptr);
				sprintf_s(SNumber , 14,"%I64d", (int64_t) memcache->sequence) ;
				leveldb_put( memcache->db, write_options, "SEQUENCENUMBER", 15, SNumber, strlen(SNumber)+1, &errptr);
			}
		}
		zloop_timer (bstar_zloop (clonesrv->bstar), 1000, 0, s_flush_ttl, base);
	}
	return 0;
}

static int
	s_new_passive (zloop_t *loop, zmq_pollitem_t *unused, void *args)
{
	clonesrv_t *clonesrv;
	char* errptr ;
	int baseid;
	int cacheid;
	zmq_pollitem_t poller = { 0, 0, 0 };
	clonesrv = (clonesrv_t *) args;
	clonesrv->active = FALSE;
	clonesrv->passive = TRUE;
	clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "ClusterName %s (%s) s_new_passive", clonesrv->ClusterName, clonesrv->ServerType);
	for (baseid = 0; baseid < clonesrv->nbr_bases; baseid++) {
		base_t *base = clonesrv->bases[baseid];
		// ?? zloop_timer_end (bstar_zloop (clonesrv->bstar), clonesrv);
		for (cacheid = 0; cacheid < base->nbr_memcaches; cacheid++)
		{
			memcache_t *memcache = base->memcaches [cacheid];
			zhash_destroy (&memcache->kvmap);
			//// destroy database
			leveldb_destroy_db( memcache->dbOptions, memcache->dbPath, &errptr);
		}

		//  Start subscribing to updates
		//zmq_pollitem_t poller = { base->subscriber, 0, ZMQ_POLLIN };
		poller.socket = base->subscriber;
		poller.events = ZMQ_POLLIN;
		zloop_poller (bstar_zloop (clonesrv->bstar), &poller, s_subscriber, base);
	}
	return 0;
}

//  .split subscriber handler
//  When we get an update, we create a new kvmap if necessary, and then
//  add our update to our kvmap. We're always passive in this case:

static int
	s_subscriber (zloop_t *loop, zmq_pollitem_t *poller, void *args)
{
	int cacheid;
	char* errptr;
	char *key;
	char *value;
	int size;
	void *snapshot;
	char SNumber[14];
	base_t *base = (base_t *) args;
	clonesrv_t *clonesrv = (clonesrv_t *) base->clonesrv;
	kvmsg_t *kvmsg;
	leveldb_writeoptions_t *write_options;
	leveldb_readoptions_t *read_options;
	write_options = leveldb_writeoptions_create();

	//  Get state snapshot if necessary
	if (base->memcaches [0]->kvmap == NULL) {
		snapshot = zsocket_new (base->ctx, ZMQ_DEALER);
		zsocket_connect (snapshot, "tcp://localhost:%d", base->peer);
		clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "s_subscriber %s : asking for snapshot tcp://localhost:%d", base->baseidstr, base->peer);
		zstr_send (snapshot, "GETSNAPSHOT");
		while (TRUE) {
			kvmsg_t *kvmsg = kvmsg_recv (snapshot);
			if (!kvmsg)
				break;          //  Interrupted
			if (streq (kvmsg_key (kvmsg), "BEGINMEMCACHE")) {
				char *cacheidstr = kvmsg_get_prop (kvmsg, "cacheidstr");
				cacheid = base_getcacheid (base, cacheidstr);
				if (base->memcaches [cacheid]->kvmap == NULL) {
					base->memcaches [cacheid]->kvmap = zhash_new ();
				}
				clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "s_subscriber %s : Received BEGINMEMCACHE from: tcp://localhost:%d cacheid %s", base->baseidstr, base->peer, base->memcaches [cacheid]->cacheidstr);
				kvmsg_destroy (&kvmsg);
			}  else if (streq (kvmsg_key (kvmsg), "ENDSNAPSHOT")) {
				base->memcaches [cacheid]->sequence = kvmsg_sequence (kvmsg);
				clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "s_subscriber %s : Received ENDSNAPSHOT from: tcp://localhost:%d cacheid %s", base->baseidstr, base->peer, base->memcaches [cacheid]->cacheidstr);
				//////db Persistence db Persistence db Persistence
				sprintf_s(SNumber , 14,"%I64d", (int64_t)base->memcaches [cacheid]->sequence) ;
				leveldb_put( base->memcaches [cacheid]->db, write_options, "SEQUENCENUMBER", 15, SNumber, strlen(SNumber)+1, &errptr) ;
				//////db Persistence db Persistence db Persistence
				kvmsg_destroy (&kvmsg);
				clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "s_subscriber : Received ENDSNAPSHOT");
				break;          //  Done
			} else {
				key= kvmsg_key(kvmsg) ;
				size = kvmsg_size(kvmsg) ;
				if (size) {
					value= (char*) kvmsg_body(kvmsg) ;
				}
				else {
					value = "";
				}
				clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "s_subscriber %s : Received DATA from: tcp://localhost:%d cacheid %s", base->baseidstr, base->peer, base->memcaches [cacheid]->cacheidstr);
				leveldb_put( base->memcaches [cacheid]->db, write_options, key, strlen(key)+1 , value, strlen(value)+1, &errptr) ;
				kvmsg_store (&kvmsg, base->memcaches [cacheid]->kvmap);
			}
		}
		zsocket_destroy (base->ctx, snapshot);
	}
	//  Find and remove update off pending list
	kvmsg = kvmsg_recv (poller->socket);
	if (!kvmsg)
		return 0;

	if (streq (kvmsg_key (kvmsg), "HUGZ")) {
		kvmsg_destroy (&kvmsg);
	} else {
		char *cacheidstr = kvmsg_get_prop (kvmsg, "cacheidstr");
		cacheid = base_getcacheid (base, cacheidstr);
		if (!s_was_pending (base->memcaches [cacheid], kvmsg)) {
			//  If active update came before client update, flip it
			//  around, store active update (with sequence) on pending
			//  list and use to clear client update when it comes later
			zlist_append (base->memcaches [cacheid]->pending, kvmsg_dup (kvmsg));
		}
		//  If update is more recent than our kvmap, apply it
		if (kvmsg_sequence (kvmsg) > base->memcaches [cacheid]->sequence) {
			base->memcaches [cacheid]->sequence = kvmsg_sequence (kvmsg);
			key= kvmsg_key(kvmsg) ;
			size = kvmsg_size(kvmsg) ;
			if (size) {
				value= (char*) kvmsg_body(kvmsg) ;
			}
			else {
				value = "";
			}
			//////db Persistence
			leveldb_put( base->memcaches [cacheid]->db, write_options, key, strlen(key)+1 , value, strlen(value)+1, &errptr) ;
			sprintf_s(SNumber, 14 ,"%I64d", (int64_t) base->memcaches [cacheid]->sequence) ;
			leveldb_put( base->memcaches [cacheid]->db, write_options, "SEQUENCENUMBER", 15, SNumber, strlen(SNumber)+1, &errptr) ;
			//////db Persistence

			kvmsg_store (&kvmsg, base->memcaches [cacheid]->kvmap);
		}
		else {
			clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "s_subscriber %s :received update out of sequence destroy it", base->baseidstr)  ;
			kvmsg_destroy (&kvmsg);
		}
	}
	return 0;
}
