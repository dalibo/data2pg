FROM php:7.4-apache

RUN apt-get update

RUN apt-get install -y libpq-dev

RUN docker-php-ext-install pgsql
