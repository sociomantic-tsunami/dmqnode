# Duplicate definition of DIST before FROM is needed to be able to use it
# in docker image name:
ARG  DIST=xenial
FROM sociomantictsunami/develdlang:$DIST-v6 as builder
# Copies the whole project as makd needs git history:
COPY . /project/
WORKDIR /project/
# Redefine arguments as env vars to be used inside build.sh script:
ARG DIST=xenial
ARG DMD=2.078.*
ENV DMD=$DMD DIST=$DIST
RUN docker/build.sh

ARG DIST=xenial
# For now plain ubuntu image is used as base for simplicity. It certainly can be
# optimized to bare minimum but right now it is not important:
FROM sociomantictsunami/runtimebase:$DIST-v6

# Set up directories and install the node
COPY --from=builder /project/build/production/pkg/ /packages/
COPY docker/install.sh /
RUN /install.sh && rm /install.sh

# Copy example configuration files. Note, that this directory should
# be mounted into the container for real world usage
COPY ./doc/etc /etc/dmqnode
# Need custom entry point to set up /etc/credentials file before running
EXPOSE 10000 10001
WORKDIR /srv/dmqnode/dmqnode-0
CMD [ "dmqnode", "-c", "etc/config.ini" ]
