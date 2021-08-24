.PHONY: help
help: ## Display callable targets.
	@echo "Reference card for usual actions."
	@echo "Here are available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: all ## Initializes data2pg.
all: stop build up database-init

.PHONY: init ## Initializes data2pg.
init: init-database test-init-database init-schema test-init-schema

.PHONY: init-database ## Initializes data2pg.
init-database:
	bash data2pg_init_db.sh

.PHONY: test-init-database ## Initializes data2pg.
test-init-database:
	bash test_pg/1-init.sh

.PHONY: init-schema ## Initializes data2pg.
init-schema:
	bash data2pg_init_schema.sh

.PHONY: test-init-schema ## Initializes data2pg.
test-init-schema:
	bash test_pg/3-configure.sh

################################################################################
#######################DOCKER###################################################
################################################################################

.PHONY: database-init ## Initializes database.
database-init:
	docker-compose exec -T database make init

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
################################################################################
################################################################################

################################################################################
#######################TEST inside CI#########################################################
################################################################################
.PHONY: dc-test ## Test data2pg.
dc-test: ## Test data2pg.
	@docker-compose exec --user postgres -T database make test

#########################f#######################################################
#######################TEST local#########################################################
################################################################################
.PHONY: test ## Test data2pg.
test: test-batch-0 test-batch-1 test-batch-compare ## Test data2pg.

.PHONY: test-batch-0 ## Test batch0.
test-batch-0: ## Test batch0.
	perl data2pg.pl --conf test_pg/batch0.conf --action run

.PHONY: test-batch-compare ## Test batch0.
test-batch-compare: ## Test batch0.
	perl data2pg.pl --conf test_pg/batch_compare.conf --action run

.PHONY: test-batch-1 ## Test batch1.
test-batch-1: ## Test batch1.
	perl data2pg.pl --conf test_pg/batch1.conf --action run
