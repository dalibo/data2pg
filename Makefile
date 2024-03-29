ifndef CI_PROJECT_PATH
	export ABSOLUTE_PATH=.
endif
.PHONY: help
help: ## Display callable targets.
	@echo "Reference card for usual actions."
	@echo "Here are available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: all ## Initializes data2pg.
all: stop build up

.PHONY: init-database ## Initializes data2pg.
init-database: init-admin-db init-test-database init-extension configure-test-migration

.PHONY: init-admin-db ## Initializes the data2pg administration database.
init-admin-db:
	cd ext/ ; sudo make install
	bash data2pg_init_admin_db.sh

.PHONY: test-init-database ## Initializes postgres test environment.
init-test-database:
	bash test_pg/1-init.sh

.PHONY: init-extension ## Initializes the data2pg extension.
init-extension:
	bash data2pg_init_extension.sh

.PHONY: configure-test-migration ## Configure the test migration.
configure-test-migration:
	bash test_pg/3-configure.sh

################################################################################
#######################     DOCKER     #########################################
################################################################################

.PHONY: dc-init-database ## Initializes database.
dc-init-database:
	docker-compose exec -T database make init-database

.PHONY: rm ## delete all containers.
rm: ## delete all container.
	@docker-compose rm --force

.PHONY: build ## Build all containers.
build: docker-compose.yml
	@docker-compose pull --ignore-pull-failures
	@docker-compose build --force-rm --pull

.PHONY: up ## Up all containers.
up: docker-compose.yml
	@docker-compose up --detach --remove-orphans

.PHONY: start ## Start the project
start: build up

.PHONY: stop ## Remove docker containers
stop:
	@docker-compose stop
	@docker-compose kill
	@docker-compose rm -v --force

.PHONY: reset ## Reset the project
reset: stop start

.PHONY: restart ## Restart all containers
restart:
		@docker-compose down
		@docker-compose up --detach --build --force-recreate

################################################################################
#######################     Tests inside CI     ################################
################################################################################
.PHONY: dc-test ## Test data2pg.
dc-test: ## Test data2pg.
	@docker-compose exec --user postgres -T database make test

#########################f######################################################
#######################     Manual tests     ###################################
################################################################################
.PHONY: test ## Test data2pg.
test: test-batch-0 test-batch-1 test-batch-tables-check test-batch-compare test-batch-discover

.PHONY: test-batch-0 ## Test batch0.
test-batch-0: ## Test batch0.
	perl data2pg.pl --conf test_pg/batch0.conf --user data2pg_adm --action run

.PHONY: test-batch-1 ## Test batch1.
test-batch-1: ## Test batch1.
	perl data2pg.pl --conf test_pg/batch1.conf --user data2pg_adm --action run

.PHONY: test-batch-compare ## Test batch_compare.
test-batch-compare: ## Test batch_compare.
	perl data2pg.pl --conf test_pg/batch_compare.conf --user data2pg_adm --action run

.PHONY: test-batch-tables-check ## Test batch_tables_check.
test-batch-tables-check: ## Test batch_tables_check.
	perl data2pg.pl --conf test_pg/batch_tables_check.conf --user data2pg_adm --action run

.PHONY: test-batch-discover ## Test batch_discover.
test-batch-discover: ## Test batch_discover.
	perl data2pg.pl --conf test_pg/batch_discover.conf --user data2pg_adm --action run
