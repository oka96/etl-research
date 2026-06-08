FROM apache/seatunnel:2.3.10 AS seatunnel

FROM eclipse-temurin:17-jre
COPY --from=seatunnel /opt/seatunnel /opt/seatunnel
WORKDIR /opt/seatunnel
