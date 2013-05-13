/*  =========================================================================
    clone_log - record log data

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


#ifndef __CLONE_LOG_H_INCLUDED__
#define __CLONE_LOG_H_INCLUDED__

#define LOG_LEVEL_FATAL             5
#define LOG_LEVEL_CRITIC            4
#define LOG_LEVEL_ERROR             3
#define LOG_LEVEL_WARNING           2
#define LOG_LEVEL_INFO              1

#define LOG_TYPE_SYSTEM             1
#define LOG_TYPE_NETWORK            2
#define LOG_TYPE_PERSIST            3
#define LOG_TYPE_SECURITY           4
#define LOG_TYPE_PRINTING           5
#define LOG_TYPE_APPSERVER          6
#define LOG_TYPE_CLONE              7
#define LOG_TYPE_APPLICATION        8
#define LOG_TYPE_OTHERS             9

#ifdef __cplusplus
extern "C" {
#endif

//  Constructor  Connect log to remote endpoint
void
    clone_log_new ();

//  Destructor
void
    clone_log_destroy ();

void
	clone_logString (int level, int type, char *body);

//  Record one log event
void
    clone_log (int level, int type, char *format, ...);

void
	clone_printString (char *fileName, const char *key, char *body);

#ifdef __cplusplus
}
#endif

#endif
