FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build-env

RUN rm -fr /src && mkdir -p /src
COPY CamusDB.Core /src/CamusDB.Core/
COPY CamusDB /src/CamusDB/

RUN cd /src/CamusDB/ && dotnet publish --no-self-contained --configuration Release -p:RunAnalyzers=false -o /app

FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS runtime
WORKDIR /app

EXPOSE 5095
EXPOSE 7070

COPY --from=build-env /app .
COPY docker/config.cluster.yml /app/Config/config.yml
COPY docker/certs/development-certificate.pfx /app/certificate.pfx

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

ENTRYPOINT ["sh", "-c", "dotnet CamusDB.dll \
  --mode=cluster \
  --http-port=${CAMUS_HTTP_PORT:-5095} \
  --raft-nodename=${CAMUS_RAFT_NODENAME} \
  --raft-nodeid=${CAMUS_RAFT_NODEID:-1} \
  --raft-host=${CAMUS_RAFT_HOST} \
  --raft-port=${CAMUS_RAFT_PORT:-7070} \
  --raft-certificate=/app/certificate.pfx \
  --initial-cluster-partitions=${CAMUS_INITIAL_CLUSTER_PARTITIONS:-3} \
  --initial-cluster ${CAMUS_INITIAL_CLUSTER}"]
