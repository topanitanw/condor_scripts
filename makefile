PYTHON=$(shell which python)

#############################################################################
requirements.txt:
	pipreqs . --force

install:
	pip install pipreqs
	pip install -r requirements.txt
.PHONY: install

install-yapf:
	conda install conda-forge::yapf
.PHONY: install-yapf

format:
	yapf --in-place scripts/bin/*.py;
.PHONY: format

#############################################################################

ME_ONLY=no
STATUS_FLAG=
ifeq ($(ME_ONLY), yes)
	STATUS_FLAG+= -m
endif

status:
	$(PYTHON) scripts/bin/submitted_job_status.py $(STATUS_FLAG)
.PHONY: status

avail:
	$(PYTHON) scripts/bin/server_availability.py
.PHONY: avail
