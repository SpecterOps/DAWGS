EXTENSION = dawgs_p5_native_adjacency_v1
MODULE_big = dawgs_p5_native_adjacency_v1
OBJS = dawgs_p5_native_adjacency_v1.o
DATA = sql/dawgs_p5_native_adjacency_v1--1.0.sql

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

PG_CFLAGS += -Wall -Werror

include $(PGXS)
