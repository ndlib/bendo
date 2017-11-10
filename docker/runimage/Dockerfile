# Bendo Container - runs on port 14000
# Requires:
#	$BENDO_RPM
#	$RUNIT_RPM
#       run.app and run.log in host docker workdir
#       config and tokens files in host docker workdir

#CentOs 6 Base

ARG CENTOS_VERSION=6
FROM centos:${CENTOS_VERSION}

# Create app user (do we need this anymore?)

RUN /usr/sbin/adduser -u 1518 app

# Get bendo from S3 bucker where codebuild put it 
# (I'm specifying a version here- get this from ENV, provide latest tag?)

ARG BENDO_RPM
RUN yum -y install https://s3.amazonaws.com/bendo-rpm-bucket-1640xqmtgjc7s/bendo/rpms/$BENDO_RPM

# Install runit service manager from  its repo 

ARG RUNIT_RPM
RUN yum -y install https://packagecloud.io/imeyer/runit/packages/el/6/$RUNIT_RPM/download.rpm

# Use /opt/sv as the runit service directory, /var/log/bendo as log directory

RUN mkdir -p /opt/sv/bendo/log
RUN mkdir -p /var/log/bendo

# copy files to start bendo application and logger
ADD ./run.app /opt/sv/bendo/run
ADD ./run.log /opt/sv/bendo/log/run

RUN chmod -R +x /opt/sv/bendo

#Copy bendo config files to /op/bendo on container

ADD ./tokens /opt/bendo/tokens
ADD ./config /opt/bendo/config

# Creat dirs for Bendo Tape, Cache

RUN mkdir -p /mnt/bendo/{tape,cache}
 
#add bendo to runit service monitoring

RUN ln -s /opt/sv/bendo /etc/service

#Start it up!

CMD ["/sbin/runsvdir", "/etc/service"]

