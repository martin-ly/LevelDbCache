/*  =====================================================================
  clone - client-side Clone Pattern class

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

#include "stdafx.h"
#include "bstar.h"
#include "clone.h"
#include "clone_log.h"


//  If no server replies within this time, abandon request
#define GLOBAL_TIMEOUT  4000    //  msecs
#define DEBUG FALSE

//  =====================================================================
//  Synchronous part, works in our application thread

//  ---------------------------------------------------------------------


//  This is the thread that handles our real clone class
static void clone_agent (void *args, zctx_t *ctx, void *pipe);
static memcache_t *memcache_new (uint cacheid);
static void	memcache_destroy (memcache_t **memcache_p);

void AddListnerForSnapshot(clone_t *clone,PRETURNUNCALLBACENDSNAPSHOT pReturnCallbcksnapshot)
{
	assert (clone);
	clone->pReturnCallbcksnapshot= pReturnCallbcksnapshot ;
} 

void AddListnerForUpdate(clone_t *clone,PRETURNUNCALLBACKUPDATE pReturnCallbckupdate)
{
	assert (clone);
	clone->pReturnCallbckupdate= pReturnCallbckupdate ;
}

//  .split constructor and destructor
//  Constructor and destructor for the clone class:

clone_t *
	clone_new (char *confPath)
{
	clone_t *clone;
	char *logthreadstate;
	char *clonethreadstate;
	extern struct clone_parameters *params;

	init_parameters ();
	parse_config (confPath);
	zclock_log("I: clone_new...");
	clone = (clone_t *) zmalloc (sizeof (clone_t));
	clone->ctx = zctx_new ();
	//  Register our logs handlers
	clone_log_new();

	clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: clone_new params->ClusterName : %s params->ModuleName : %s...", params->ClusterName, params->ModuleName);
	clone->pipe = zthread_fork (clone->ctx, clone_agent, clone);
	clonethreadstate = zstr_recv(clone->pipe);
	assert (streq (clonethreadstate, "ready"));
	return clone;
}

void
	clone_destroy (clone_t **clone_p)
{
	assert (clone_p);
	if (*clone_p) {
		clone_t *clone = *clone_p;
		clone_log_destroy();
		zctx_destroy (&clone->ctx);
		free (clone);
		*clone_p = NULL;
	}
}

//  .split subtree method
//  Specify subtree for snapshot and updates, do before connect.
//  Sends [SUBTREE][subtree] to the agent:

void clone_subtree (clone_t *clone, char *subtree)
{
	zmsg_t *msg;
	char *clonethreadstate;
	assert (clone);
	msg = zmsg_new ();
	zmsg_addstr (msg, "SUBTREE");
	zmsg_addstr (msg, subtree);
	zmsg_send (&msg, clone->pipe);
	clonethreadstate = zstr_recv(clone->pipe);
	assert (streq (clonethreadstate, "ready"));
}

//  .split connect method
//  Connect to new server endpoint.
//  Sends [CONNECT][endpoint][service] to the agent:

void
	clone_connect_server (clone_t *clone, char *address, char *port)
{
	zmsg_t *msg;
	char *clonethreadstate;
	assert (clone);
	msg = zmsg_new ();
	zmsg_addstr (msg, "CONNECT");
	zmsg_addstr (msg, address);
	zmsg_addstr (msg, port);
	zmsg_send (&msg, clone->pipe);
	clonethreadstate = zstr_recv(clone->pipe);
	assert (streq (clonethreadstate, "ready"));
}

void
	clone_connect (clone_t *clone)
{
	extern struct clone_parameters *params;
	//baseid set to 0 because clone client is connected to only one base
	base_parameters *base_params = params->bases[0];
	clone_connect_server (clone, base_params->addressprimary, base_params->portprimary);
	clone_connect_server (clone, base_params->addressbackup, base_params->portbackup);
}

static int
	s_print_single (const char *key, void *data, void *args)
{
	int64_t sequence;
	char *body;
	int size;
	char *fileName;
	extern struct clone_parameters *params;
	memcache_t *memcache = (memcache_t *) args;
	kvmsg_t *kvmsg = (kvmsg_t *) data;
	size = kvmsg_size(kvmsg) ;
	sequence = kvmsg_sequence (kvmsg);
	if (size) {
		body= (char*) kvmsg_body(kvmsg) ;
	}
	else {
		body = "";
	}
	fileName = (char *) malloc ( sizeof(params->logPath) + sizeof(params->ModuleName) + sizeof(memcache->cacheidstr) + sizeof(DUMP_EXT) + 4 * sizeof(char) +1 );
	size = snprintf(NULL, 0 , "%s%s%s%s", params->logPath, params->ModuleName, memcache->cacheidstr, DUMP_EXT);
	snprintf(fileName, size + 1, "%s%s%s%s", params->logPath, params->ModuleName, memcache->cacheidstr, DUMP_EXT);
	//sscanf (kvmsg_get_prop (kvmsg, "ttl"), "%I64d", &ttl);
	//clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: s_print_single cache=%s seq:%I64d key:%s body:%s", memcache->cacheidstr, sequence, key, body);
	clone_printString(fileName, key, body);
	free(fileName);
	return 0;
}

static int
	s_print_kvm (memcache_t *memcache)
{
	assert(memcache);
	clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: s_print_kvm cache=%s", memcache->cacheidstr);
	if (memcache->kvmap)
		zhash_foreach (memcache->kvmap, s_print_single, memcache);
	return 0;
}

//  .split set method
//  Set new value in distributed hash table.
//  Sends [SET][key][value][cacheid] to the agent:

void
	clone_set (clone_t *clone, char *cacheidstr, char *key, char *value, int ttl)
{
	zmsg_t *msg;
	char *clonethreadstate;
	char ttlstr [10];
	char *fileName;
	int size;
	FILE *fp;
	extern struct clone_parameters *params;
	assert (clone);

	sprintf (ttlstr, "%d", ttl);
	if (DEBUG) {
		fileName = (char *) malloc ( sizeof(params->logPath) + sizeof(params->ModuleName) + sizeof(cacheidstr) + sizeof(SET_EXT) + 4 * sizeof(char) +1 );
		size = snprintf(NULL, 0 , "%s%s%s", params->logPath, params->ModuleName, cacheidstr, SET_EXT);
		snprintf(fileName, size + 1, "%s%s%s", params->logPath, params->ModuleName, cacheidstr, SET_EXT);
		clone_printString (fileName, key, value);
		free(fileName);
	}
	msg = zmsg_new ();
	zmsg_addstr (msg, "SET");
	zmsg_addstr (msg, key);	
	zmsg_addstr (msg, cacheidstr);
	zmsg_addstr (msg, value);
	zmsg_addstr (msg, ttlstr);
	zmsg_send (&msg, clone->pipe);
	clonethreadstate = zstr_recv(clone->pipe);
	//clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "UC clone_set assert ready get : %s", clonethreadstate);
	assert (streq (clonethreadstate, "ready"));
}

//  .split get method
//  Lookup value in distributed hash table.
//  Sends [GET][key] to the agent and waits for a value response.
//  If there is no clone available, will eventually return NULL:

char *
	clone_get (clone_t *clone, char *cacheidstr, char *key)
{
	zmsg_t *msg;
	zmsg_t *reply;
	char *clonethreadstate;

	assert (clone);
	assert (key);
	msg = zmsg_new ();
	zmsg_addstr (msg, "GET");
	zmsg_addstr (msg, key);
	zmsg_addstr (msg, cacheidstr);
	zmsg_send (&msg, clone->pipe);

	reply = zmsg_recv (clone->pipe);
	if (reply) {
		char *value = zmsg_popstr (reply);
		zmsg_destroy (&reply);
		return value;
	}
	return NULL;
}

//  .split working with servers
//  The back-end agent manages a set of servers, which we implement using
//  our simple class model:

typedef struct {
	char *address;              //  Server address
	int port;                   //  Server port
	void *snapshot;             //  Snapshot socket
	void *subscriber;           //  Incoming updates
	uint64_t expiry;            //  When server expires
	uint requests;              //  How many snapshot requests made?
} server_t;

static server_t *
	server_new (zctx_t *ctx, char *address, int port, char *subtree)
{
	int snapshot_result, subscriber_result;
	server_t *server = (server_t *) zmalloc (sizeof (server_t));
	server->address = strdup (address);
	server->port = port;

	//DEALER SNAPSHOTS SUB
	clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: server_new adding snapshot new");
	server->snapshot = zsocket_new (ctx, ZMQ_DEALER);
	clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: server_new adding snapshot connect");
	snapshot_result = zsocket_connect (server->snapshot, "%s:%d", address, port);
	//UPDATE SUB
	clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: server_new adding subscriber new");
	server->subscriber = zsocket_new (ctx, ZMQ_SUB);
	clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: server_new adding subscriber connect");
	subscriber_result = zsocket_connect (server->subscriber, "%s:%d", address, port + 1);
	clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: server_new adding server %s:%d... snapshot_result=%d subscriber_result=%d subtree=%s", address, port, snapshot_result, subscriber_result, subtree);
	zsockopt_set_subscribe (server->subscriber, subtree);
	clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: server_new RETURN");
	return server;
}

static void
	server_destroy (server_t **server_p)
{
	assert (server_p);
	if (*server_p) {
		server_t *server = *server_p;
		free (server->address);
		free (server);
		*server_p = NULL;
	}
}

//  .split back-end agent class
//  Here is the implementation of the back-end agent itself:

//  Number of servers we will talk to
#define SERVER_MAX      2

//  Server considered dead if silent for this long
#define SERVER_TTL      5000    //  msecs

//  States we can be in
#define STATE_INITIAL       0   //  Before asking server for state
#define STATE_SYNCING       1   //  Getting state from server
#define STATE_ACTIVE        2   //  Getting new updates from server

typedef struct {
	zctx_t *ctx;                //  Context wrapper
	void *pipe;                 //  Pipe back to application
	memcache_t *memcaches [CACHE_MAX];          //  memcache TABLEAU
	uint nbr_memcaches;         //  0 to CACHE_MAX
	char cacheids [CACHE_MAX][MAXLEN]; 
	char *subtree;              //  Subtree specification, if any
	server_t *server [SERVER_MAX];
	uint nbr_servers;           //  0 to SERVER_MAX
	uint state;                 //  Current state
	uint cur_server;            //  If active, server 0 or 1
	int64_t sequence;           //  Last kvmsg processed
	void *publisher;            //  Outgoing updates
	PRETURNUNCALLBACENDSNAPSHOT pReturnCallbcksnapshot;
	PRETURNUNCALLBACKUPDATE pReturnCallbckupdate;
} agent_t;

static void
	agent_addcache (agent_t *agent, char *cacheidstr)
{
	assert (agent);
	strcpy(agent->cacheids[agent->nbr_memcaches], cacheidstr);
	agent->memcaches [agent->nbr_memcaches] = memcache_new (agent->nbr_memcaches);
	agent->nbr_memcaches++;
}

static agent_t *
	agent_new (zctx_t *ctx, void *pipe)
{	
	extern struct clone_parameters *params;
	int cacheid;
	//Client is connected to only one base
	base_parameters *base_params = params->bases[0];
	agent_t *agent = (agent_t *) zmalloc (sizeof (agent_t));
	agent->ctx = ctx;
	agent->pipe = pipe;
	agent->cur_server = 0;
	for (cacheid = 0; cacheid < base_params->nbr_memcaches; cacheid++) {
		agent_addcache (agent, base_params->cacheids[cacheid]);
	}
	agent->subtree = strdup ("");
	agent->state = STATE_INITIAL;
	agent->publisher = zsocket_new (agent->ctx, ZMQ_PUB);
	return agent;
}

static void
	agent_destroy (agent_t **agent_p)
{
	assert (agent_p);
	if (*agent_p) {
		agent_t *agent = *agent_p;
		int server_nbr;
		int cacheid;
		for (server_nbr = 0; server_nbr < agent->nbr_servers; server_nbr++)
			server_destroy (&agent->server [server_nbr]);
		for (cacheid = 0; cacheid < agent->nbr_memcaches; cacheid++)
			memcache_destroy (&agent->memcaches [cacheid]);
		free (agent->subtree);
		free (agent);
		*agent_p = NULL;
	}
}

static memcache_t *
	agent_getcache (agent_t *agent, char *cacheidstr)
{
	int cacheid;
	assert (agent);
	for (cacheid = 0; cacheid < agent->nbr_memcaches; cacheid++)
		if (streq (cacheidstr, agent->cacheids[cacheid]))
			return agent->memcaches [cacheid];
	return NULL;
}

static int
	agent_getcacheid (agent_t *agent, char *cacheidstr)
{
	int cacheid;
	assert (agent);
	for (cacheid = 0; cacheid < agent->nbr_memcaches; cacheid++)
		if (streq (cacheidstr, agent->cacheids[cacheid]))
			return cacheid;
	return -1;
}

//  .split handling a control message
//  Here we handle the different control messages from the front-end;
//  SUBTREE, CONNECT, SET, and GET:

static int
	agent_control_message (agent_t *agent)
{
	int result;
	zmsg_t *msg = zmsg_recv (agent->pipe);
	char *command = zmsg_popstr (msg);
	if (command == NULL)
		return -1;      //  Interrupted
	//clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: agent_control_message command %s",command);
	if (streq (command, "SUBTREE")) {
		free (agent->subtree);
		agent->subtree = zmsg_popstr (msg);
	}
	else if (streq (command, "CONNECT")) {
		char *address = zmsg_popstr (msg);
		char *port = zmsg_popstr (msg);
		if (agent->nbr_servers < SERVER_MAX) {
			agent->server [agent->nbr_servers] = server_new (agent->ctx, address, atoi (port), agent->subtree);
			//  We broadcast updates to all known servers PUB UPDATES
			result = zsocket_connect (agent->publisher, "%s:%d", address, atoi (port) + 2);
			clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: agent_control_message CONNECT to %s:%s RESULT=%d server %u",address, port, result, agent->nbr_servers);
			if (result == 0)
				agent->nbr_servers++;
		}
		else
			clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "E: agent_control_message too many servers (max. %d)", SERVER_MAX);
		free (address);
		free (port);
	}
	else if (streq (command, "SET")) {
		//  .split set and get commands
		//  When we set a property, we push the new key-value pair onto
		//  all our connected servers:
		kvmsg_t *kvmsg;
		FILE *fp;
		char *key = zmsg_popstr (msg);
		char *cacheidstr = zmsg_popstr (msg);
		char *value = zmsg_popstr (msg);
		char *ttlStr = zmsg_popstr (msg);
		//  Send key-value pair on to server
		kvmsg = kvmsg_new (0);
		kvmsg_set_prop (kvmsg, "cacheidstr", cacheidstr);
		kvmsg_set_key  (kvmsg, key);
		kvmsg_set_uuid (kvmsg);
		kvmsg_fmt_body (kvmsg, "%s", value);
		kvmsg_set_prop (kvmsg, "ttl", ttlStr);
		kvmsg_send     (kvmsg, agent->publisher);
		kvmsg_destroy (&kvmsg);
		free (cacheidstr);
		free (ttlStr);
		free (key);             //  Value is owned by hash table
	}
	else if (streq (command, "GET")) {
		char *value = NULL;
		memcache_t *memcache = NULL;
		char *key = zmsg_popstr (msg);
		char *cacheidstr = zmsg_popstr (msg);
		//LECTURE en local
		if (strneq (cacheidstr, "")) {
			memcache = agent_getcache (agent, cacheidstr);
			value = (char* ) zhash_lookup (memcache->kvmap, key);
		}
		if (value)
			zstr_send (agent->pipe, value);
		else
			zstr_send (agent->pipe, "");
		free (key);
		free (value);
	}
	else {
		clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "E: agent_control_message unknown command : %s ", command);
	}
	zmsg_destroy (&msg);
	if (strneq (command, "GET"))
		zstr_send (agent->pipe, "ready");
	free (command);
	return 1;
}

//  .split back-end agent
//  The asynchronous agent manages a server pool and handles the
//  request/reply dialog when the application asks for it:

static void
	clone_agent (void *args, zctx_t *ctx, void *pipe)
{
	char *body;
	memcache_t *memcache;
	int cacheid;
	char* errptr;
	char *key;
	char *value;
	zmq_pollitem_t poll_set [] = {
		{ pipe, 0, ZMQ_POLLIN, 0 },
		{ 0,    0, ZMQ_POLLIN, 0 }
	};
	int poll_size = 1;
	char SNumber[14];	
	Bool initial = TRUE;
	agent_t *agent = agent_new (ctx, pipe);
	clone_t *clnt = (clone_t *) args;
	while (TRUE) {
		int rc;
		int size;
		int poll_timer = -1;
		server_t *server = agent->server [agent->cur_server];
		agent->pReturnCallbcksnapshot= clnt->pReturnCallbcksnapshot;
		agent->pReturnCallbckupdate= clnt->pReturnCallbckupdate;

		if (server) {
			switch (agent->state) {
			case STATE_INITIAL:
				//  In this state we ask the server for a snapshot,
				//  if we have a server to talk to...
				poll_set [1].socket = server->subscriber;
				break;

			case STATE_SYNCING:
				//  In this state we read from snapshot and we expect
				//  the server to respond, else we fail over.
				poll_set [1].socket = server->snapshot;
				break;

			case STATE_ACTIVE:
				//  In this state we read from subscriber and we expect
				//  the server to give hugz, else we fail over.
				poll_set [1].socket = server->subscriber;
				break;
			}
			server->expiry = zclock_time () + SERVER_TTL;
			poll_timer = (server->expiry - zclock_time ()) * ZMQ_POLL_MSEC;
			if (poll_timer < 0)
				poll_timer = 0;
		}
		//  .split client poll loop
		//  We're ready to process incoming messages; if nothing at all
		//  comes from our server within the timeout, that means the
		//  server is dead:
		if (initial) {
			zstr_send (pipe, "ready");
			initial=FALSE;
		}
		//clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: clone_agent zmq_poll poll_timer=%d", poll_timer);
		rc = zmq_poll (poll_set, poll_size, poll_timer);
		if (rc == -1)
			break;              //  Context has been shut down
		//Do not manage commands during sync of snapshot
		if (((agent->nbr_servers < SERVER_MAX && agent->state == STATE_INITIAL) || agent->state == STATE_ACTIVE) && poll_set [0].revents & ZMQ_POLLIN) {
			//clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: clone_agent recv POLLIN");
			if (agent_control_message (agent))
				if (agent->nbr_servers > 0) {
					clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: clone_agent recv ready to go nbr_servers=%d cur_server=%d", agent->nbr_servers, agent->cur_server);
					poll_size = 2;
				} else {
					clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: clone_agent recv problem creating servers");
				}
			else
				break;          //  Interrupted
		}
		else if (poll_set [1].revents & ZMQ_POLLIN) {
			char * cacheidstr;
			kvmsg_t *kvmsg = kvmsg_recv (poll_set [1].socket);
			//memcache_t *memcache = NULL;	
			//clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: clone recv POLLIN");
			if (!kvmsg)
				break;          //  Interrupted

			//  Anything from server resets its expiry time
			server->expiry = zclock_time () + SERVER_TTL;
			//clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: clone_agent STATE=%d nbr_servers=%d cur_server %u", agent->state, agent->nbr_servers, agent->cur_server);
			switch (agent->state) {
			case STATE_INITIAL:
				//  In this state we ask the server for a snapshot,
				//  if we have a server to talk to...
				if (agent->nbr_servers > 0) {
					clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: waiting for server at '%s':'%d'requests %u...", server->address, server->port, server->requests);
					if (agent->memcaches [0]->kvmap == NULL && server->requests < 2) {
						clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "s_subscriber asking for snapshot GETSNAPSHOT");
						zstr_send (server->snapshot, "GETSNAPSHOT");
						server->requests++;
					}
					agent->state = STATE_SYNCING;
					poll_set [1].socket = server->snapshot;
				}
				else
					poll_size = 1;
				break;
			case STATE_SYNCING:
				//  Store in snapshot until we're finished
				server->requests = 0;
				clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: waiting for server at '%s':'%d'requests %u...", server->address, server->port, server->requests);
				cacheidstr = kvmsg_get_prop (kvmsg, "cacheidstr");
				if (strneq (cacheidstr, ""))
					cacheid = agent_getcacheid (agent, cacheidstr); //FIXME manage error
				if (streq (kvmsg_key (kvmsg), "BEGINMEMCACHE")) {	
					if (agent->memcaches [cacheid]->kvmap == NULL) {
						agent->memcaches [cacheid]->kvmap = zhash_new ();
					}
					clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "s_subscriber Received BEGINMEMCACHE");
					kvmsg_destroy (&kvmsg);
				}  else if (streq (kvmsg_key (kvmsg), "ENDSNAPSHOT")) {
					agent->sequence =  kvmsg_sequence (kvmsg);
					agent->state = STATE_ACTIVE;
					kvmsg_destroy (&kvmsg);
					s_print_kvm (agent->memcaches [cacheid]);
					clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "s_subscriber Received ENDSNAPSHOT BREAK !!!!!!!!!!!!");
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
					clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "s_subscriber Received DATA cacheid=%d value=%s", cacheid, value);
					if(agent->pReturnCallbcksnapshot) {
						//clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: received from %s:%d cacheid %s SEND pReturnCallbcksnapshot", server->address, server->port, memcache->cacheidstr);
						(agent->pReturnCallbcksnapshot)(kvmsg_key(kvmsg), value);
					}
					kvmsg_store (&kvmsg, agent->memcaches [cacheid]->kvmap);
				}
				//} // if poll
				//} //while
				break;
			case STATE_ACTIVE:
				//  In this state we read from subscriber and we expect
				//  the server to give hugz, else we fail over.
				//poll_set [1].socket = server->subscriber;
				//  Discard out-of-sequence updates, incl. hugz
				if (strneq (kvmsg_key (kvmsg), "HUGZ")) {
					cacheidstr = kvmsg_get_prop (kvmsg, "cacheidstr");
					if (strneq (cacheidstr, ""))
						memcache = agent_getcache (agent, cacheidstr);
					if (!memcache)
						break;          //  Interrupted
					if (kvmsg_sequence (kvmsg) > agent->sequence) {
						agent->sequence = kvmsg_sequence (kvmsg);
						size = kvmsg_size(kvmsg) ;
						if (size) {
							body= (char*) kvmsg_body(kvmsg) ;
						}
						else {
							body = "";
						}
						if(agent->pReturnCallbckupdate) {
							//clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: execute pReturnCallbckupdate msg %s", body);
							(agent->pReturnCallbckupdate)(kvmsg_key(kvmsg), body);
						}
						kvmsg_store (&kvmsg, memcache->kvmap);
						//clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: SNAPSHOT memcache cacheid=%s size=%u", memcache->cacheidstr, zhash_size (memcache->kvmap));
					}
					else {
						clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: HUGZ out of sequence last=%I64d got=%I64d", agent->sequence, kvmsg_sequence (kvmsg));
						kvmsg_destroy (&kvmsg);
					}
				}
				else
					kvmsg_destroy (&kvmsg);
				break;
			}
		}
		else {
			//  Server has died, failover to next
			server->expiry = zclock_time () + SERVER_TTL;
			//clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: server at %s:%d didn't give hugz CUR_SERVER=%d", server->address, server->port, agent->cur_server);
			agent->cur_server = (agent->cur_server + 1) % agent->nbr_servers;
			// Reinit kvmap before resynchro
			for (cacheid = 0; cacheid < agent->nbr_memcaches; cacheid++) {
				memcache = agent->memcaches [cacheid];
				memcache->kvmap=NULL;
			}
			agent->state = STATE_INITIAL;
		}
	}
	agent_destroy (&agent);
}

static memcache_t *
	memcache_new (uint cacheid)
{
	extern struct clone_parameters *params;
	//Client is connected to only one base
	base_parameters *base_params = params->bases[0];
	memcache_t *memcache = (memcache_t *) zmalloc (sizeof (memcache_t));
	clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: clone memcache_new MEMCACHE_NEW %d", cacheid);
	strncpy (memcache->cacheidstr, base_params->cacheids[cacheid], MAXLEN);
	//memcache->kvmap = zhash_new ();
	memcache->pending = zlist_new ();
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
