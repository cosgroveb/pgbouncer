
PG_CPPFLAGS = -I$(shell pg_config --includedir)
PG_LIBS = -lpq
PG_LDFLAGS = -L$(shell pg_config --libdir)

USUAL_DIR = ../lib

SUBLOC = test
DIST_SUBDIRS = ssl


noinst_PROGRAMS = hba_test tls_server_end_point_test
hba_test_CPPFLAGS = -I../include $(LIBEVENT_CFLAGS)
hba_test_LDADD = $(LIBEVENT_LIBS) $(TLS_LIBS)
hba_test_CFLAGS = -O0
hba_test_SOURCES = hba_test.c ../src/hba.c ../src/util.c
hba_test_EMBED_LIBUSUAL = 1

tls_server_end_point_test_CPPFLAGS = -I../include $(TLS_CPPFLAGS)
tls_server_end_point_test_LDFLAGS = $(TLS_LDFLAGS)
tls_server_end_point_test_LDADD = $(TLS_LIBS)
tls_server_end_point_test_CFLAGS = -O0
tls_server_end_point_test_SOURCES = tls_server_end_point_test.c
tls_server_end_point_test_EMBED_LIBUSUAL = 1

EXTRA_PROGRAMS = asynctest
asynctest_CPPFLAGS = -I../include $(PG_CPPFLAGS) $(LIBEVENT_CFLAGS)
asynctest_LDFLAGS = $(PG_LDFLAGS)
asynctest_LDADD = $(PG_LIBS) $(LIBEVENT_LIBS)
asynctest_SOURCES = asynctest.c
asynctest_EMBED_LIBUSUAL = 1

AM_FEATURES = libusual


include ../config.mak
include ../lib/mk/antimake.mk

check: all
	./hba_test
	./tls_server_end_point_test
