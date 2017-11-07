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
RUN /install-ruby.sh ${RUBY_VERSION}
RUN gem install fpm

COPY install-go.sh /
ARG GOLANG_VERSION=1.7.3
ARG GOLANG_DOWNLOAD_SHA256=508028aac0654e993564b6e2014bf2d4a9751e3b286661b0b0040046cf18028e
RUN /install-go.sh ${GOLANG_VERSION} ${GOLANG_DOWNLOAD_SHA256}
