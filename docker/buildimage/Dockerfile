ARG CENTOS_VERSION=6
FROM centos:${CENTOS_VERSION}
RUN yum -y groupinstall "Development tools" \
 ; yum -y install git ImageMagick \
 ; yum -y install libxslt-devel libyaml-devel libxml2-devel gdbm-devel \
      libffi-devel zlib-devel openssl-devel libyaml-devel readline-devel \
      curl-devel openssl-devel pcre-devel memcached-devel valgrind-devel \
      mysql-devel ImageMagick-devel \
 ; yum clean all

COPY install-ruby.sh /
ARG RUBY_VERSION=2.4.1
ARG RUBY_GEMS_VERSION=2.6.12
RUN /install-ruby.sh ${RUBY_VERSION} ${RUBY_GEMS_VERSION}
RUN gem install fpm

COPY install-go.sh /
ARG GOLANG_VERSION=1.12.9
ARG GOLANG_DOWNLOAD_SHA256=ac2a6efcc1f5ec8bdc0db0a988bb1d301d64b6d61b7e8d9e42f662fbb75a2b9b
RUN /install-go.sh ${GOLANG_VERSION} ${GOLANG_DOWNLOAD_SHA256}
