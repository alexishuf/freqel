all: image
MVN_DEFS ?= -Dgroups=fast

.PHONY: clean freqel-server

clean:
	rm -f freqel-server
	docker image rm  "alexishuf/freqel:$(git rev-parse HEAD | head -c 8)" || true

freqel-server:
	cd .. &&\
 		./mvnw package $(MVN_DEFS) &&\
 		cp freqel-server/target/freqel-server docker/freqel-server

image: Dockerfile freqel-server
	docker build -t "alexishuf/freqel:$$(git rev-parse HEAD | head -c 8)" .
	docker tag "alexishuf/freqel:$$(git rev-parse HEAD | head -c 8)" alexishuf/freqel:latest

