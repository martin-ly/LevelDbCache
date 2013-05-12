/*  =====================================================================
*  bstar - Binary Star reactor
*  ===================================================================== */

#include "stdafx.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "bstar.h"
#include "clone.h"
#include "clone_log.h"


//  States we can be in at any point in time
typedef enum {
	STATE_PRIMARY = 1,          //  Primary, waiting for peer to connect
	STATE_BACKUP = 2,           //  Backup, waiting for peer to connect
	STATE_ACTIVE = 3,           //  Active - accepting connections
	STATE_PASSIVE = 4           //  Passive - not accepting connections
} state_t;

//  Events, which start with the states our peer can be in
typedef enum {
	PEER_PRIMARY = 1,           //  HA peer is pending primary
	PEER_BACKUP = 2,            //  HA peer is pending backup
	PEER_ACTIVE = 3,            //  HA peer is active
	PEER_PASSIVE = 4,           //  HA peer is passive
	SNAPSHOT_REQUEST = 5             //  Client makes request
} event_t;


//  Structure of our class

struct _bstar_t {
	zctx_t *ctx;                //  Our private context
	zloop_t *loop;              //  Reactor loop
	void *statepub;             //  State publisher
	void *statesub;             //  State subscriber
	state_t state;              //  Current state
	event_t event;              //  Current event
	int64_t peer_expiry;        //  When peer is considered 'dead'
	zloop_fn *snapshot_fn;         //  Voting socket handler
	void *snapshot_arg;            //  Arguments for voting handler
	zloop_fn *active_fn;        //  Call when become active
	void *active_arg;           //  Arguments for handler
	zloop_fn *passive_fn;       //  Call when become passive
	void *passive_arg;          //  Arguments for handler
};

struct clone_parameters *params;
//  The finite-state machine is the same as in the proof-of-concept server.
//  To understand this reactor in detail, first read the CZMQ zloop class.
//  .skip

//  We send state information every this often
//  If peer doesn't respond in two heartbeats, it is 'dead'
#define BSTAR_HEARTBEAT     1000        //  In msecs

//  ---------------------------------------------------------------------
//  Binary Star finite state machine (applies event to state)
//  Returns -1 if there was an exception, 0 if event was valid.

static int
	s_execute_fsm (bstar_t *bstar)
{
	int rc = 0;
	//  Primary server is waiting for peer to connect
	//  Accepts SNAPSHOT_REQUEST events in this state
	if (bstar->state == STATE_PRIMARY) {
		if (bstar->event == PEER_BACKUP) {
			clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: connected to backup (passive), ready as active");
			bstar->state = STATE_ACTIVE;
			if (bstar->active_fn)
				(bstar->active_fn) (bstar->loop, NULL, bstar->active_arg);
		}
		else if (bstar->event == PEER_ACTIVE) {
			clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: connected to backup (active), ready as passive");
			bstar->state = STATE_PASSIVE;
			if (bstar->passive_fn)
				(bstar->passive_fn) (bstar->loop, NULL, bstar->passive_arg);
		}
		else if (bstar->event == SNAPSHOT_REQUEST) {
			// Allow client requests to turn us into the active if we've
			// waited sufficiently long to believe the backup is not
			// currently acting as active (i.e., after a failover)
			clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: request from client, ready as active");
			assert (bstar->peer_expiry > 0);
			if (zclock_time () >= bstar->peer_expiry) {
				clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: s_execute_fsm SNAPSHOT_REQUEST");
				bstar->state = STATE_ACTIVE;
				if (bstar->active_fn)
					(bstar->active_fn) (bstar->loop, NULL, bstar->active_arg);
			} else
				// Don't respond to clients yet - it's possible we're
				// performing a failback and the backup is currently active
				rc = -1;
		}
	}
	else if (bstar->state == STATE_BACKUP) {
		//  Backup server is waiting for peer to connect
		//  Rejects SNAPSHOT_REQUEST events in this state
		if (bstar->event == PEER_ACTIVE) {
			clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: connected to primary (active), ready as passive");
			bstar->state = STATE_PASSIVE;
			if (bstar->passive_fn)
				(bstar->passive_fn) (bstar->loop, NULL, bstar->passive_arg);
		}
		else if (bstar->event == SNAPSHOT_REQUEST)
			rc = -1;
	}
	else if (bstar->state == STATE_ACTIVE) {
		//  Server is active
		//  Accepts SNAPSHOT_REQUEST events in this state
		//  The only way out of ACTIVE is death

		if (bstar->event == PEER_ACTIVE) {
			//  Two actives would mean split-brain
			clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "E: fatal error - dual actives, aborting");
			rc = -1;
		}
	}
	else if (bstar->state == STATE_PASSIVE) {
		//  Server is passive
		//  SNAPSHOT_REQUEST events can trigger failover if peer looks dead
		if (bstar->event == PEER_PRIMARY) {
			//  Peer is restarting - become active, peer will go passive
			clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: primary (passive) is restarting, ready as active");
			bstar->state = STATE_ACTIVE;
		}
		else if (bstar->event == PEER_BACKUP) {
			//  Peer is restarting - become active, peer will go passive
			clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: backup (passive) is restarting, ready as active");
			bstar->state = STATE_ACTIVE;
		}
		else if (bstar->event == PEER_PASSIVE) {
			//  Two passives would mean cluster would be non-responsive
			clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "E: fatal error - dual passives, aborting");
			rc = -1;
		}
		else if (bstar->event == SNAPSHOT_REQUEST) {
			//  Peer becomes active if timeout has passed
			//  It's the client request that triggers the failover
			//  FIXME filter vote from clients coming from the same machine
			assert (bstar->peer_expiry > 0);
			if (zclock_time () >= bstar->peer_expiry) {
				//  If peer is dead, switch to the active state
				clone_log(LOG_LEVEL_INFO, LOG_TYPE_CLONE, "I: failover successful, ready as active");
				bstar->state = STATE_ACTIVE;
			}
			else
				//  If peer is alive, reject connections
				rc = -1;
		}
		//  Call state change handler if necessary
		if (bstar->state == STATE_ACTIVE && bstar->active_fn)
			(bstar->active_fn) (bstar->loop, NULL, bstar->active_arg);
	}
	return rc;
}

static void
	s_update_peer_expiry (bstar_t *bstar)
{
	bstar->peer_expiry = zclock_time () + 2 * BSTAR_HEARTBEAT;
}

//  ---------------------------------------------------------------------
//  Reactor event handlers...

//  Publish our state to peer
int s_send_state (zloop_t *loop, zmq_pollitem_t *poller, void *arg)
{
	bstar_t *bstar = (bstar_t *) arg;
	// ATTENTION zstr_send ?
	zstr_sendf (bstar->statepub, "%d", bstar->state);
	return 0;
}

//  Receive state from peer, execute finite state machine
int s_recv_state (zloop_t *loop, zmq_pollitem_t *poller, void *arg)
{
	bstar_t *bstar = (bstar_t *) arg;
	char *state = zstr_recv (poller->socket);
	if (state) {
		bstar->event = (event_t) atoi (state);
		s_update_peer_expiry (bstar);
		free (state);
	}
	return s_execute_fsm (bstar);
}


int s_snapshot_request (zloop_t *loop, zmq_pollitem_t *poller, void *arg)
{
	bstar_t *bstar = (bstar_t *) arg;

	//  If server receive a snapshot request
	bstar->event = SNAPSHOT_REQUEST;
	zclock_log("I: s_snapshot_request s_execute_fsm event=SNAPSHOT_REQUEST...");
	//snapshot_fn is send_snapshot function
	if (s_execute_fsm (bstar) == 0)
		(bstar->snapshot_fn) (bstar->loop, poller, bstar->snapshot_arg);
	else {
		//  Destroy waiting message, no-one to read it
		zmsg_t *msg = zmsg_recv (poller->socket);
		zmsg_destroy (&msg);
	}
	return 0;
}

//  .until
//  .split constructor
//  This is the constructor for our bstar class. We have to tell it whether
//  we're primary or backup server, and our local and remote endpoints to
//  bind and connect to:

bstar_t * 
	bstar_new (int primary, char *local, char *remote)
{
	bstar_t	*bstar;
	zmq_pollitem_t poller;

	bstar = (bstar_t *) zmalloc (sizeof (bstar_t));

	//  Initialize the Binary Star
	bstar->ctx = zctx_new ();
	bstar->loop = zloop_new ();
	bstar->state = primary? STATE_PRIMARY: STATE_BACKUP;

	//  Create publisher for state going to peer
	bstar->statepub = zsocket_new (bstar->ctx, ZMQ_PUB);
	zsocket_bind (bstar->statepub, local);

	//  Create subscriber for state coming from peer
	bstar->statesub = zsocket_new (bstar->ctx, ZMQ_SUB);
	zsockopt_set_subscribe (bstar->statesub, "");
	zsocket_connect (bstar->statesub, remote);

	//  Set-up basic reactor events
	zloop_timer (bstar->loop, BSTAR_HEARTBEAT, 0, s_send_state, bstar);
	//zmq_pollitem_t poller = { bstar->statesub, 0, ZMQ_POLLIN };
	poller.socket= bstar->statesub ;
	poller.fd=0 ;
	poller.events= ZMQ_POLLIN;
	zclock_log("I: BSTAR bstar_new zloop_poller s_recv_state...");
	zloop_poller (bstar->loop, &poller, s_recv_state, bstar);
	return bstar;
}


//  .split destructor
//  The destructor shuts down the bstar reactor:

void
	bstar_destroy (bstar_t **bstar_p)
{
	assert (bstar_p);
	if (*bstar_p) {
		bstar_t *bstar = *bstar_p;
		zloop_destroy (&bstar->loop);
		zctx_destroy (&bstar->ctx);
		free (bstar);
		*bstar_p = NULL;
	}
}


//  .split zloop method
//  The zloop method returns the underlying zloop reactor, so we can add
//  additional timers and readers:

zloop_t *
	bstar_zloop (bstar_t *bstar)
{
	return bstar->loop;
}


//  .split voter method
//  The snapshot_req_receptor method registers a client router socket. Messages received
//  on this socket provide the SNAPSHOT_REQUEST events for the Binary Star
//  FSM and are passed to the provided application handler in charge to manage snapshot requests. We require
//  exactly one snapshot_req_receptor per bstar instance:

int
	bstar_snapshot_req_receptor (bstar_t *bstar, char *endpoint, int type, zloop_fn send_snapshot,	void *arg)
{
	zmq_pollitem_t poller;
	void *router_socket;
	zclock_log ("I: clonesrv bstar_snapshot_req_receptor !! %d %s", type, endpoint);
	router_socket = zsocket_new (bstar->ctx, type);
	//  Hold actual handler+arg so we can call this later
	zsocket_bind (router_socket, endpoint);
	assert (!bstar->snapshot_fn);
	bstar->snapshot_fn = send_snapshot;
	bstar->snapshot_arg = arg;
	//zmq_pollitem_t poller = { socket, 0, ZMQ_POLLIN };
	poller.socket= router_socket ;
	poller.fd=0 ;
	poller.events= ZMQ_POLLIN;
	return zloop_poller (bstar->loop, &poller, s_snapshot_request, bstar);
}


//  .split register state-change handlers
//  Register handlers to be called each time there's a state change:

void
	bstar_new_active (bstar_t *bstar, zloop_fn handler, void *arg)
{
	assert (!bstar->active_fn);
	bstar->active_fn = handler;
	bstar->active_arg = arg;
}

void
	bstar_new_passive (bstar_t *bstar, zloop_fn handler, void *arg)
{
	assert (!bstar->passive_fn);
	bstar->passive_fn = handler;
	bstar->passive_arg = arg;
}

//  .split enable/disable tracing
//  Enable/disable verbose tracing, for debugging:

void bstar_set_verbose (clonesrv_t *clonesrv, Bool verbose)
{
	zloop_set_verbose (bstar_zloop (clonesrv->bstar), verbose);
}

//  .split start the reactor
//  Finally, start the configured reactor. It will end if any handler
//  returns -1 to the reactor, or if the process receives SIGINT or SIGTERM:

int
	bstar_start (bstar_t *bstar)
{
	
	int result;
	zloop_t *loop;
	//assert (bstar->snapshot_fn);
	assert (bstar->loop);
	s_update_peer_expiry (bstar);
	return zloop_start (bstar->loop);
}

/*
* initialize data to default values
*/
void
	init_parameters ()
{
	extern struct clone_parameters *params;
	params = (struct clone_parameters *) zmalloc (sizeof (struct clone_parameters));
	params->primary = FALSE;
	strncpy (params->baseids[0], "0", MAXLEN);
	strncpy (params->logPath, "D:/tmp/log", MAXLEN);
	strncpy (params->ClusterName, "Default", MAXLEN);
	strncpy (params->ModuleName, "Default", MAXLEN);
	strncpy (params->ServerType, "Backup", MAXLEN);
	strncpy (params->bstarLocal, "tcp://*:5004", MAXLEN);
	strncpy (params->bstarRemote, "tcp://localhost:5003", MAXLEN);
}

void
	init_base_parameters (char *baseName)
{
	extern struct clone_parameters *params;
	//Client is connected to only one base
	base_parameters *base_params;
	base_params = (struct _base_parameters *) zmalloc (sizeof (struct _base_parameters));
	strncpy (base_params->baseidstr , baseName, MAXLEN);
	base_params->port = 5556;
	base_params->peer = 5566;
	base_params->nbr_memcaches = 1;
	strncpy (base_params->databasePath, "D:/tmp/databaseBackup", MAXLEN);
	strncpy (base_params->bstarReceptor, "tcp://*:5566", MAXLEN);
	strncpy (base_params->addressprimary, "tcp://localhost", MAXLEN);
	strncpy (base_params->portprimary, "5556", MAXLEN);
	strncpy (base_params->addressbackup, "tcp://localhost", MAXLEN);
	strncpy (base_params->portbackup, "5566", MAXLEN);
	strncpy (base_params->cacheids[0] , "0", MAXLEN);
	params->bases[params->nbr_bases] = base_params;
}

/*
* trim: get rid of trailing and leading whitespace...
*       ...including the annoying "\n" from fgets()
*/
char *
	trim (char * s)
{
	/* Initialize start, end pointers */
	char *s1 = s, *s2 = &s[strlen (s) - 1];

	/* Trim and delimit right side */
	while ( (isspace (*s2)) && (s2 >= s1) )
		s2--;
	*(s2+1) = '\0';

	/* Trim left side */
	while ( (isspace (*s1)) && (s1 < s2) )
		s1++;

	/* Copy finished string */
	strcpy (s, s1);
	return s;
}

/*
* parse external parameters file
*
* - Attention:
*   a) Parameters should be initialized to reasonable defaults
*   b) Pararmeters should be validated whether they are complete, or correct.
*/


void
	parse_base_config (char * params_filePath, char *baseName)
{
	extern struct clone_parameters *params;
	char *s, buff[256];
	char path[256];
	base_parameters *base_params;
	int pathLen;
	FILE *fp;
	base_params = params->bases[params->nbr_bases];
	pathLen = snprintf(NULL, 0 , "%s.%s",params_filePath, baseName);
	snprintf(path, pathLen +1, "%s.%s",params_filePath, baseName) ;
	zclock_log ("E: parse_base_config parsing %s", path);
	fp = fopen (path, "r");
	if (fp == NULL)
	{
		zclock_log ("E: parse_base_config fp NULL!!");
		return;
	}
	
	strncpy (base_params->baseidstr, baseName, MAXLEN);
	/* Read next line */
	while ((s = fgets (buff, sizeof buff, fp)) != NULL)
	{
		char name[MAXLEN], value[MAXLEN];
		/* Skip blank lines and comments */
		if (buff[0] == '\n' || buff[0] == '#')
			continue;

		/* Parse name/value pair from line */
		s = strtok (buff, "=");
		if (s==NULL)
			continue;
		else
			strncpy (name, s, MAXLEN);
		s = strtok (NULL, "=");
		if (s==NULL)
			continue;
		else
			strncpy (value, s, MAXLEN);
		trim (value);

		/* Copy into correct entry in parameters struct */
		if (streq(name, "port"))
			base_params->port = atoi(value);
		else if (streq(name, "peer"))
			base_params->peer = atoi(value);
		else if (streq(name, "databasePath")) {
			strncpy (base_params->databasePath, value, MAXLEN);
		}
		else if (streq(name, "cacheids")) {
			char *token=strtok(value, ",");
			base_params->nbr_memcaches = 0;
			while(token != NULL) {
				zclock_log ("E: parse_base_config cacheids name %s value %s token %s", name, value, token);
				strncpy (base_params->cacheids[base_params->nbr_memcaches], token, MAXLEN);
				base_params->nbr_memcaches++;
				token=strtok(NULL, ",");
				zclock_log ("E: parse_base_config cacheids2 name %s value %s token %s", name, value, token);
			}
		}
		else if (streq(name, "bstarReceptor"))
			strncpy (base_params->bstarReceptor, value, MAXLEN);
		else if (streq(name, "addressprimary"))
			strncpy (base_params->addressprimary, value, MAXLEN);
		else if (streq(name, "portprimary"))
			strncpy (base_params->portprimary, value, MAXLEN);
		else if (streq(name, "addressbackup"))
			strncpy (base_params->addressbackup, value, MAXLEN);
		else if (streq(name, "portbackup"))
			strncpy (base_params->portbackup, value, MAXLEN);
		else
			zclock_log ("E: %s/%s: Unknown name/value pair!", name, value);
	}
	/* Close file */
	fclose (fp);
	zclock_log ("I: parse_base_config databasePath: %s, port: %d, peer: %d", base_params->databasePath, base_params->port, base_params->peer);
}

void
	parse_config (char * params_filePath)
{
	char *s, buff[256];
	extern struct clone_parameters *params;
	FILE *fp = fopen (params_filePath, "r");

	zclock_log ("E: parse_config parsing %s", params_filePath);
	if (fp == NULL)
	{
		zclock_log ("E: parse_config fp NULL!!");
		return;
	}

	/* Read next line */
	while ((s = fgets (buff, sizeof buff, fp)) != NULL)
	{
		char name[MAXLEN], value[MAXLEN];
		/* Skip blank lines and comments */
		if (buff[0] == '\n' || buff[0] == '#')
			continue;

		/* Parse name/value pair from line */
		s = strtok (buff, "=");
		if (s==NULL)
			continue;
		else
			strncpy (name, s, MAXLEN);
		s = strtok (NULL, "=");
		if (s==NULL)
			continue;
		else
			strncpy (value, s, MAXLEN);
		trim (value);
		zclock_log ("E: parse_config name %s value %s!", name, value);
		/* Copy into correct entry in parameters struct */
		if (streq(name, "primary")) {
			if (streq(value, "TRUE"))
				params->primary = TRUE;
		}
		else if (streq(name, "logPath")) {
			strncpy (params->logPath, value, MAXLEN);
		}
		else if (streq(name, "ClusterName"))
			strncpy (params->ClusterName, value, MAXLEN);
		else if (streq(name, "ModuleName"))
			strncpy (params->ModuleName, value, MAXLEN);
		else if (streq(name, "ServerType"))
			strncpy (params->ServerType, value, MAXLEN);
		else if (streq(name, "bstarLocal"))
			strncpy (params->bstarLocal, value, MAXLEN);
		else if (streq(name, "bstarRemote"))
			strncpy (params->bstarRemote, value, MAXLEN);
		else if (streq(name, "baseidstrs")) {
			char *token=strtok(value, ",");
			params->nbr_bases = 0;
			zclock_log ("E: parse_config baseidstrs name %s value %s token %s", name, value, token);
			while(token != NULL) {
				zclock_log ("E: parse_config parse_base_config %s %s", params_filePath, token);
				init_base_parameters(token);
				parse_base_config(params_filePath, token);
				strncpy (params->baseids[params->nbr_bases++], token, MAXLEN);
				token=strtok(NULL, ",");
			}
		}
		else
			zclock_log ("E: %s/%s: Unknown name/value pair!", name, value);
	}
	/* Close file */
	fclose (fp);
	zclock_log ("I: parse_config primary: %d, ServerType=%s, bstarLocal=%s bstarRemote=%s", params->primary, params->ServerType, params->bstarLocal, params->bstarRemote);
}