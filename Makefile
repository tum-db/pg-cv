CURRENT_DIR=$(shell pwd)

up:
	docker rm -v plpython3 | true
	docker build -f plpython3.dockerfile -t plpython3 .
	docker run -p 5432:5432 -v $(CURRENT_DIR)/src:/src:ro -v $(CURRENT_DIR)/benchmark:/benchmark:ro -v $(CURRENT_DIR)/data:/data:ro --name plpython3 plpython3

down:
	docker stop plpython3
