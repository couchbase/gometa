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

IF (TARGET forestdb)
  LIST (APPEND _deps forestdb)
ENDIF ()
IF (NOT "${_deps}" STREQUAL "")
  SET (_deps DEPENDS ${_deps})
ENDIF ()

GoInstall (TARGET gometa PACKAGE github.com/couchbase/gometa/cmd/gometa
  GOPATH "${PROJECT_SOURCE_DIR}/../../../.." "${GODEPSDIR}"
  ${_deps}
  CGO_INCLUDE_DIRS "${FORESTDB_INCLUDE_DIR}"
  CGO_LIBRARY_DIRS "${FORESTDB_LIBRARY_DIR}"
  INSTALL_PATH bin OUTPUT gometa)
