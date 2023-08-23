PYTHON=$(shell which python)

avail:
	$(PYTHON) scripts/bin/server_availability.py
.PHONY: avail

status:
	$(PYTHON) scripts/bin/submitted_job_status.py
.PHONY: status

requirements.txt:
	pipreqs . --force

format:
	yapf --in-place *.py;
.PHONY: format
