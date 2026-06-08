FROM eclipse-temurin:17-jre AS jre

FROM apache/beam_python3.11_sdk:latest
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"
RUN pip install --no-cache-dir kafka-python==2.2.15
