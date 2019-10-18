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
ARG GOLANG_VERSION=1.13.3
ARG GOLANG_DOWNLOAD_SHA256=0804bf02020dceaa8a7d7275ee79f7a142f1996bfd0c39216ccb405f93f994c0
RUN /install-go.sh ${GOLANG_VERSION} ${GOLANG_DOWNLOAD_SHA256}
