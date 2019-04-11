CMAKE_MINIMUM_REQUIRED (VERSION 2.8)
PROJECT (gometa)

# This project is intended to be built as part of a Couchbase Server build,
# but for nominal support building stand-alone, the following variables
# need to be specified:
#
# CMAKE_MODULE_PATH - should contain a directory containing
#      FindCouchbaseGo.cmake, eg. tlm/cmake/Modules
#
# CMAKE_INSTALL_PREFIX - should point to a directory where the bin/ output
#      directory will be placed
#
# GODEPSPATH - should point to a Go workspace directory containing all
#      transitive Go dependencies
#
# FORESTDB_INCLUDE_DIR - should point to where libforestdb/forestdb.h exists
#
# FORESTDB_LIBRARY_DIR - should point to where libforestdb.so resides
#

INCLUDE (FindCouchbaseGo)
INCLUDE (FindCouchbaseCurl)

IF (TARGET forestdb)
  LIST (APPEND _deps forestdb)
ENDIF ()
IF (NOT "${_deps}" STREQUAL "")
  SET (_deps DEPENDS ${_deps})
ENDIF ()

GET_FILENAME_COMPONENT (CURL_LIBRARY_DIR "${CURL_LIBRARIES}" DIRECTORY)

SET (CGO_INCLUDE_DIRS "${CURL_INCLUDE_DIR};${FORESTDB_INCLUDE_DIR}")
SET (CGO_LIBRARY_DIRS "${CURL_LIBRARY_DIR};${FORESTDB_LIBRARY_DIR}")

GoInstall (TARGET gometa PACKAGE github.com/couchbase/gometa/cmd/gometa
  GOPATH "${PROJECT_SOURCE_DIR}/../../../.." "${GODEPSDIR}"
  ${_deps}
  CGO_INCLUDE_DIRS "${CGO_INCLUDE_DIRS}"
  CGO_LIBRARY_DIRS "${CGO_LIBRARY_DIRS}"
  GOVERSION 1.11.6
  INSTALL_PATH bin OUTPUT gometa)
