FROM node:18-alpine AS nodegui

WORKDIR /gui
COPY core/gui/package.json core/gui/yarn.lock ./
# Fake git-version.js during yarn install to prevent git from causing cache
# invalidation of dependencies
RUN touch git-version.js && yarn install

COPY core/gui .
# Position of .git doesn't matter since it's only there for the revision hash
COPY .git ./.git
RUN apk add --no-cache git && \
	node git-version.js && \
	apk del git && \
	yarn run build

FROM sbtscala/scala-sbt:eclipse-temurin-jammy-11.0.17_8_1.9.3_2.13.11

WORKDIR /core/amber
COPY core/amber .
RUN sbt clean package
RUN apt-get update
RUN apt-get install -y netcat unzip python3-pip
RUN pip3 install python-lsp-server python-lsp-server[websockets]
RUN pip3 install -r requirements.txt
RUN pip3 install -r operator-requirements.txt

WORKDIR /core
COPY core/scripts ./scripts
# Add .git for runtime calls to jgit from OPversion
COPY .git ../.git
COPY --from=nodegui /gui/dist ./gui/dist

RUN scripts/build-docker.sh

CMD ["scripts/deploy-docker.sh"]

EXPOSE 8080