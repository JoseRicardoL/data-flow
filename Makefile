include .env

.PHONY: validate deploy clean upload prepare test logs download

validate:
	./scripts/deploy/validate.sh $(REGION)

upload:
	./scripts/deploy/upload.sh $(LOCAL_SCRIPTS_PATH)/oracle_extraction.py $(S3_BUCKET) $(GLUE_SCRIPTS_PATH)

deploy: upload
	./scripts/deploy/deploy.sh $(STACK_NAME) $(REGION) $(ENV) $(S3_BUCKET) $(GLUE_SCRIPTS_PATH)

clean:
	./scripts/deploy/clean.sh $(STACK_NAME) $(REGION) $(ENV)

prepare:
	./scripts/deploy/prepare.sh $(STACK_NAME) $(S3_BUCKET)

test: prepare
	./scripts/deploy/test.sh $(STACK_NAME) $(REGION) $(ENV)

logs:
	./scripts/deploy/logs.sh $(STACK_NAME) $(REGION)

download:
	./scripts/deploy/download.sh $(STACK_NAME) $(REGION) $(ENV) $(S3_BUCKET)

.PHONY: local-init local-up local-down local-shell local-jupyter local-run

local-init:
	@scripts/local/init.sh

local-up:
	@scripts/local/up.sh

local-down:
	@scripts/local/down.sh

local-shell:
	@scripts/local/shell.sh

local-jupyter:
	@scripts/local/jupyter.sh

local-run:
	@scripts/local/run.sh $(SCRIPT) $(INPUT)

local-doctor:
	@scripts/local/doctor.sh