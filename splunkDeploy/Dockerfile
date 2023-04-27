FROM  splunk/splunk:latest

ENV SPLUNK_START_ARGS=--accept-license
ENV SPLUNK_PASSWORD="mypassword"

COPY test.csv /opt/splunk/var/run/splunk/csv/test.csv
COPY requests.csv /opt/splunk/var/run/splunk/csv/requests.csv
EXPOSE 8000 8089

CMD ["start"]