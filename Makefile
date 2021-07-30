.PHONY: help
help: ## Display callable targets.
	@echo "Reference card for usual actions."
	@echo "Here are available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: init-database ## Initializes data2pg.
init-database: bash data2pg_init_db.sh

.PHONY: test-init-database ## Initializes data2pg.
test-init-database: bash test_pg/1-init.sh

.PHONY: init-schema ## Initializes data2pg.
init-schema: bash data2pg_init_schema.sh

.PHONY: test-init-schema ## Initializes data2pg.
test-init-schema: bash test_pg/3-configure.sh
