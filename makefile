PYTHON=$(shell which python)

avail:
	$(PYTHON) scripts/bin/server_availability.py
.PHONY: avail

ME_ONLY=no
STATUS_FLAG=
ifeq ($(ME_ONLY), yes)
	STATUS_FLAG+= -m
endif

status:
	$(PYTHON) scripts/bin/submitted_job_status.py -d $(STATUS_FLAG)
.PHONY: status

requirements.txt:
	pipreqs . --force

format:
	yapf --in-place scripts/bin/*.py;
.PHONY: format
