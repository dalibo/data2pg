EXTENSION = data2pg data2pg_admin
DATA = $(wildcard *--*.sql)

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)