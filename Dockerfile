FROM eclipse-temurin:11
LABEL Description="stub-vtb"
ARG JAR_FILE=target/*.jar
COPY ${JAR_FILE} app.jar
EXPOSE 8099
VOLUME /logs
ENTRYPOINT ["java", "-jar", "/app.jar"]