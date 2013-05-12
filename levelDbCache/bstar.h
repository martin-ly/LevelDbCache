/*  =====================================================================
*  bstar - Binary Star reactor
*  ===================================================================== */

#ifndef __BSTAR_H_INCLUDED__
#define __BSTAR_H_INCLUDED__

#include "czmq.h"
#include "leveldb\c.h"

//  Arguments for constructor
#define BSTAR_PRIMARY   1
#define BSTAR_BACKUP    0
#define SERVER_MAX      2 //A adapter
#define BASE_MAX       16 //A adapter
#define CACHE_MAX       16 //A adapter
#define MAXLEN 255
#define DUMP_EXT "kvm"
#define SET_EXT "set"

#ifdef __cplusplus
extern "C" {
#endif

	//  Opaque class structure
	typedef struct _bstar_t bstar_t;

	struct _base_parameters {
		int port;                   //  Main port we're working on
		int peer;                   //  Main port of our peer
		int nbr_memcaches;
		char databasePath[MAXLEN];  // le path de la base de données
		char baseidstr[MAXLEN];
		char cacheids[CACHE_MAX][MAXLEN];
		char bstarReceptor[MAXLEN];
		char addressprimary[MAXLEN];
		char portprimary[MAXLEN];
		char addressbackup[MAXLEN];
		char portbackup[MAXLEN];	
	};

	typedef struct _base_parameters base_parameters;

	struct clone_parameters {
		Bool primary;				//  TRUE if we're primary
		uint nbr_bases;				//  0 to CACHE_MAX
		char baseids[BASE_MAX][MAXLEN];
		base_parameters *bases[BASE_MAX];
		char ClusterName[MAXLEN];
		char ServerType[MAXLEN];
		char ModuleName[MAXLEN];
		char bstarLocal[MAXLEN];	//to transfert state
		char bstarRemote[MAXLEN];	//to transfert state
		char logPath[MAXLEN];       // le path des logs
	};

	typedef struct {
		char cacheidstr[MAXLEN+16]; //  id of cache
		void *base;		        // server
		zhash_t *kvmap;             //  Key-value store
		int64_t sequence;           //  How many updates we're at
		zlist_t *pending;           //  Pending updates from clients
		leveldb_t *db ;             //Persistence datatbase
		leveldb_options_t *dbOptions; //persistence Options
		char *dbPath;              // path de la base de données
	} memcache_t;
	
	//  Our server is defined by these properties
	typedef struct {
		zctx_t *ctx;                //  Context wrapper
		void *clonesrv;          //  memcache TABLEAU
		int baseid;
		char baseidstr[MAXLEN + 16]; //  id of cache
		uint nbr_memcaches;         //  0 to CACHE_MAX
		memcache_t *memcaches [CACHE_MAX];          //  memcache TABLEAU
		char cacheids [CACHE_MAX][MAXLEN];
		int port;                   //  Main port we're working on
		int peer;                   //  Main port of our peer
		void *publisher;            //  Publish updates and hugz
		void *collector;            //  Collect updates from clients
		void *subscriber;           //  Get updates from peer
	} base_t;

		//  Our server is defined by these properties
	typedef struct {
		zctx_t *ctx;                //  Context wrapper
		uint nbr_bases;         //  0 to CACHE_MAX
		base_t *bases [BASE_MAX];
		char baseids [BASE_MAX][MAXLEN];
		bstar_t *bstar;             //  Bstar reactor core
		Bool primary;               //  TRUE if we're primary
		Bool active;                //  TRUE if we're active
		Bool passive;               //  TRUE if we're passive
		char* ClusterName;
		char* ServerType;
		char logPath[MAXLEN];       // le path des logs
	} clonesrv_t;

	//  Create a new Binary Star instance, using local (bind) and
	//  remote (connect) endpoints to set-up the server peering.
	bstar_t *bstar_new (int primary, char *local, char *remote);

	//  Destroy a Binary Star instance
	void bstar_destroy (bstar_t **bstar_p);

	//  Return underlying zloop reactor, for timer and reader
	//  registration and cancelation.
	zloop_t *bstar_zloop (bstar_t *bstar);

	//  Register voting reader
	int bstar_snapshot_req_receptor (bstar_t *bstar, char *endpoint, int type, zloop_fn send_snapshot, void *arg);

	//  Register main state change handlers
	void bstar_new_active (bstar_t *bstar, zloop_fn handler, void *arg);
	void bstar_new_passive (bstar_t *bstar, zloop_fn handler, void *arg);

	//  Enable/disable verbose tracing
	void bstar_set_verbose (clonesrv_t *clonesrv, Bool verbose);

	//  Start the reactor, ends if a callback function returns -1, or the
	//  process received SIGINT or SIGTERM.
	int bstar_start (bstar_t *bstar);

	void init_parameters ();

	void parse_config (char * params_filePath);

#ifdef __cplusplus
}
#endif

extern struct clone_parameters *params;

#endif
