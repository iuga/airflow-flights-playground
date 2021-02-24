BASE := $(shell /bin/pwd)

help:
	@echo ''
	@echo 'Usage: make [TARGET] [EXTRA_ARGUMENTS]'
	@echo 'Targets:'
	@echo '  build    	build docker image'
	@echo '  serve    	run as service'
	@echo '  deploy    	deploy the project'
	@echo ''

local-database:
	docker exec -it airflow-flights-playground_mysql-datalake_1 mysql datalake --user=fligoo --password=fligoo

local-build:
	docker-compose build

local-serve:
	docker-compose up

hurry: 
	# Run full workflow for the first time
	$(MAKE) build
	$(MAKE) serve
