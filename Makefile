SHELL:=bash

aws_profile=default
aws_region=eu-west-2

default: help

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: bootstrap
bootstrap: ## Bootstrap local environment for first use
	@make git-hooks

.PHONY: git-hooks
git-hooks: ## Set up hooks in .githooks
	@git submodule update --init .githooks ; \
	git config core.hooksPath .githooks

setup-local:
	virtualenv --python=python3.8 venv
	source venv/bin/activate
	pip3 install -r requirements.txt

env-vars: ## Make env vars required by application
	@{ \
		export PYTHONPATH=$(shell pwd)/src; \
		export LOG_LEVEL=DEBUG; \
		export ENVIRONMENT=LOCAL; \
		export APPLICATION="batch_job_launcher_lambda"; \
	}

unittest:
	tox

deployable:
	rm -rf artifacts
	mkdir artifacts
	pip3 install -r requirements.txt -t artifacts
	cp src/batch_job_launcher_lambda/*.py artifacts/
	cd artifacts && zip -r ../batch-job-launcher-development.zip ./ && cd -

clean:
	rm -rf artifacts ./src/batch_job_launcher.egg-info ./batch-job-launcher-development.zip
