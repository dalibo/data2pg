FROM postgres:13.3

RUN apt update && apt-get install -y sudo nano
RUN apt-get install -y make perl
RUN apt-get install -y libperl-dev
RUN apt-get install -y libdbi-perl
RUN apt-get install -y libtest-simple-perl
RUN apt-get install -y libtest-nowarnings-perl
RUN apt-get install -y libdbd-pg-perl

COPY ./scripts /docker-entrypoint-initdb.d
