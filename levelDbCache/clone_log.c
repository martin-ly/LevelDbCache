/*  =========================================================================
clone_log - Envoie des logs

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
#include <czmq.h>
#include "bstar.h"
#include "clone.h"
#include "clone_log.h"

#define flockfile(x) EnterCriticalSection(&global_log_file_lock)
#define funlockfile(x) LeaveCriticalSection(&global_log_file_lock)

#ifdef _WIN32
static CRITICAL_SECTION global_log_file_lock;
#endif // _WIN32

int64_t log_sequence;
//  ---------------------------------------------------------------------
//  Constructeur de l'objet permettant l'envoie des logs

void
	clone_log_new ()
{
#if defined(_WIN32) && !defined(__SYMBIAN32__)
	InitializeCriticalSection(&global_log_file_lock);
#endif // _WIN32
}

//  ---------------------------------------------------------------------
//  Destruction du logger

void
	clone_log_destroy ()
{
#if defined(_WIN32) && !defined(__SYMBIAN32__)
	InitializeCriticalSection(&global_log_file_lock);
#endif // _WIN32
}

//  ---------------------------------------------------------------------
//  Envoie d'un log

void
	clone_logString (int level, int type, char *body)
{
	FILE *fp;
	time_t curtime;
	int size;
	char timestr [256];
	char fileSuffix [256];
	char *logfileName;
	char * log;
	int timeLen;
	int dateLen;
	int logLen;
	struct tm *loctime;
	extern struct clone_parameters *params;
	curtime = time (NULL);
	loctime = localtime (&curtime);
	timeLen = strftime (timestr, sizeof(timestr), "%H:%M:%S ", loctime);
	dateLen = strftime (fileSuffix, sizeof(fileSuffix), "%y-%m-%d.log", loctime);
	logfileName = (char *) malloc ( sizeof(params->logPath) + sizeof(params->ClusterName) + sizeof(params->ModuleName) + dateLen + 4 * sizeof(char) +1 );
	logLen = snprintf(NULL, 0 , "%s %d %d %s\n",timestr, level, type, body) ;
	log = (char *) malloc (logLen + 1);
	strncpy (logfileName, params->logPath, sizeof(params->logPath));
	strcat (logfileName, params->ClusterName);
	strcat (logfileName, params->ModuleName);
	strcat (logfileName, fileSuffix);
	snprintf(log, logLen +1, "%s %d %d %s\n",timestr, level, type, body);
	perror(body);
	fp=fopen(logfileName, "at");
	if (fp != NULL) {
		flockfile(fp);
		fputs(log,fp);
		fflush(fp);
		funlockfile(fp);
		fclose(fp);
	}
	free(log);
	free(logfileName);
}

/********************/

void
	clone_log (int level, int type, char *format, ...)
{
	va_list args;
	int len;
	char * body;

	va_start(args, format);
	// _vscprintf doesn't count terminating '\0'
	len = _vscprintf(format, args) + 1;
	body = (char*) malloc( len * sizeof(char) );
	vsprintf_s(body, len, format, args);
	va_end (args);
	clone_logString (level, type, body);
	free(body);
}

void
	clone_printString (char *fileName, const char *key, char *body)
{
	FILE *fp;
	int size;
	char * kvmStr;
	int stringLen;
	stringLen = snprintf(NULL, 0 , "[%s] [%s]\n", key, body) ;
	kvmStr = (char *) malloc (stringLen + 1);
	snprintf(kvmStr, stringLen +1, "[%s] [%s]\n", key, body);
	perror(body);
	fp=fopen(fileName, "at");
	if (fp != NULL) {
		// flockfile(fp);
		fprintf(fp, "%s\n", body);
		fflush(fp);
		// funlockfile(fp);
		fclose(fp);
	}
	free(kvmStr);
}
