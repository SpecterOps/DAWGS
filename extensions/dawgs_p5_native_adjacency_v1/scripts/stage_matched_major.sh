#!/usr/bin/env sh
# Copyright 2026 Specter Ops, Inc.
# SPDX-License-Identifier: Apache-2.0

set -eu

if [ "$#" -ne 2 ]; then
    echo "usage: $0 PG_CONFIG STAGE_DIRECTORY" >&2
    exit 64
fi

pg_config_path=$1
stage_directory=$2
extension_directory=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
extension_name=dawgs_p5_native_adjacency_v1
pg_version=$($pg_config_path --version)

if [ -z "${P5_NATIVE_IMAGE_ID:-}" ]; then
    echo "P5_NATIVE_IMAGE_ID is required to bind the matched-major image identity" >&2
    exit 65
fi

case $pg_version in
    "PostgreSQL 17."*|"PostgreSQL 18."*) ;;
    *)
        echo "PG_CONFIG must identify PostgreSQL 17 or 18, got: $pg_version" >&2
        exit 66
        ;;
esac

if [ -e "$stage_directory" ] && [ -n "$(find "$stage_directory" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
    echo "stage directory must be empty: $stage_directory" >&2
    exit 67
fi

mkdir -p "$stage_directory/artifacts"

make -C "$extension_directory" PG_CONFIG="$pg_config_path" clean all
cp "$extension_directory/$extension_name.so" "$stage_directory/artifacts/$extension_name.unstripped.so"
make -C "$extension_directory" PG_CONFIG="$pg_config_path" DESTDIR="$stage_directory" install

installed_library="$stage_directory$($pg_config_path --pkglibdir)/$extension_name.so"
installed_control="$stage_directory$($pg_config_path --sharedir)/extension/$extension_name.control"
installed_sql="$stage_directory$($pg_config_path --sharedir)/extension/$extension_name--1.0.sql"

if [ ! -f "$installed_library" ] || [ ! -f "$installed_control" ] || [ ! -f "$installed_sql" ]; then
    echo "staged extension package is incomplete" >&2
    exit 68
fi

strip --strip-unneeded "$installed_library"
if [ "$(wc -c < "$installed_library")" -gt 1048576 ]; then
    echo "staged library exceeds the 1 MiB feasibility cap" >&2
    exit 69
fi

{
    printf 'postgres_version=%s\n' "$pg_version"
    printf 'pg_config=%s\n' "$pg_config_path"
    printf 'image_identity=%s\n' "$P5_NATIVE_IMAGE_ID"
    printf 'compiler=%s\n' "$($pg_config_path --cc)"
    printf 'server_headers=%s\n' "$($pg_config_path --includedir-server)"
    printf 'pkglibdir=%s\n' "$($pg_config_path --pkglibdir)"
    printf 'sharedir=%s\n' "$($pg_config_path --sharedir)"
    sha256sum "$extension_directory/dawgs_p5_native_adjacency_v1.control"
    sha256sum "$extension_directory/Makefile"
    sha256sum "$extension_directory/sql/dawgs_p5_native_adjacency_v1--1.0.sql"
    sha256sum "$extension_directory/dawgs_p5_native_adjacency_v1.c"
    sha256sum "$stage_directory/artifacts/$extension_name.unstripped.so"
    sha256sum "$installed_library"
} > "$stage_directory/artifacts/build-manifest.txt"
