# Pin the build stage to the NATIVE build platform: running the .NET SDK under QEMU
# emulation throws MSB4184 GetTargetPlatformIdentifier(''). The framework-dependent
# publish is portable and carries per-RID native assets selected at load time.
FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:10.0 AS build-env

RUN rm -fr /src && mkdir -p /src
COPY CamusDB.Core /src/CamusDB.Core/
COPY CamusDB.Grpc.Contracts /src/CamusDB.Grpc.Contracts/
COPY CamusDB /src/CamusDB/

# Scrub host-built obj/bin that slipped into the context; a host-restored
# obj/project.assets.json poisons the in-container restore for the target arch
# (MSB4184: GetTargetPlatformIdentifier('') cannot be evaluated).
RUN find /src \( -type d -name obj -o -type d -name bin \) -prune -exec rm -rf {} +

RUN cd /src/CamusDB/ && dotnet publish --no-self-contained --configuration Release -p:RunAnalyzers=false -o /app

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app

EXPOSE 5095
EXPOSE 5096
EXPOSE 7070
EXPOSE 7141

COPY --from=build-env /app .
COPY docker/config.cluster.yml /app/Config/config.yml
COPY docker/certs/development-certificate.pfx /app/certificate.pfx

# Trust the self-signed dev cert so inter-node Raft gRPC (which defaults to TLS and has no
# insecure-validation bypass) accepts peer certificates. The cert's SANs cover the node IPs
# (see docker/certs/san.cnf); installing it as a CA anchor satisfies chain validation.
COPY docker/certs/development-certificate.crt /usr/local/share/ca-certificates/camus-dev.crt
RUN update-ca-certificates

ARG CAMUS_RAFT_NODENAME
ARG CAMUS_RAFT_NODEID
ARG CAMUS_RAFT_HOST
ARG CAMUS_RAFT_PORT
ARG CAMUS_HTTP_PORT
ARG CAMUS_INITIAL_CLUSTER
ARG CAMUS_INITIAL_CLUSTER_PARTITIONS

ENV DOTNET_SYSTEM_NET_SOCKETS_INLINE_COMPLETIONS=1
ENV CAMUS_RAFT_NODENAME="$CAMUS_RAFT_NODENAME"
ENV CAMUS_RAFT_NODEID="$CAMUS_RAFT_NODEID"
ENV CAMUS_RAFT_HOST="$CAMUS_RAFT_HOST"
ENV CAMUS_RAFT_PORT="$CAMUS_RAFT_PORT"
ENV CAMUS_HTTP_PORT="$CAMUS_HTTP_PORT"
ENV CAMUS_INITIAL_CLUSTER="$CAMUS_INITIAL_CLUSTER"
ENV CAMUS_INITIAL_CLUSTER_PARTITIONS="$CAMUS_INITIAL_CLUSTER_PARTITIONS"

RUN mkdir -p /data

# Extra CLI flags appended verbatim (word-split) at the end of the command line, so a container
# can opt into flags the fixed list doesn't cover without rebuilding the image — e.g.
# CAMUS_EXTRA_ARGS="--join-existing true" boots the node as a runtime joiner of the cluster
# seeded by CAMUS_INITIAL_CLUSTER. Empty by default: the command line is unchanged.
ENV CAMUS_EXTRA_ARGS=""

ENTRYPOINT ["sh", "-c", "exec dotnet CamusDB.dll \
  --mode=cluster \
  --http-port=${CAMUS_HTTP_PORT:-5095} \
  --raft-nodename=${CAMUS_RAFT_NODENAME} \
  --raft-nodeid=${CAMUS_RAFT_NODEID:-1} \
  --raft-host=${CAMUS_RAFT_HOST} \
  --raft-port=${CAMUS_RAFT_PORT:-7070} \
  --https-certificate=/app/certificate.pfx \
  --raft-certificate=/app/certificate.pfx \
  --initial-cluster-partitions=${CAMUS_INITIAL_CLUSTER_PARTITIONS:-3} \
  --initial-cluster ${CAMUS_INITIAL_CLUSTER} \
  ${CAMUS_EXTRA_ARGS}"]
