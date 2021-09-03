FROM openjdk:11.0.11-buster

# >--- --- ---=[ Build args ]=--- --- ---<

# Clone URL for the VLog git repository
ARG VLOG_REPO=https://github.com/karmaresearch/vlog.git

# VLog commit to checkout and build
ARG VLOG_REF=v1.3.5

# Options to pass to make when building VLog, e.g., -j. By default nothing is passed
ARG VLOG_MAKE_OPTS

# Git repository to clone hdt-cpp from
ARG HDT_REPO=https://github.com/rdfhdt/hdt-cpp.git

# Which commit of hdt-cpp to checkout and build
ARG HDT_REF=332a9cc

# Flags to be passed to make when building hdt-cpp
ARG HDT_MAKE_OPTS


# >--- --- ---=[ Setup ]=--- --- ---<

# Build binaries for VLog and hdt-cpp tools.
# code, build dirs, build tools and -dev packages will not remain in the image

RUN apt-get update -y                                        &&\
    apt-get install -y cmake g++ autoconf libtool pkg-config &&\
    apt-get install -y zlib1g-dev libserd-0-0 libserd-dev    &&\
    cd /tmp                      &&\
    git clone "$VLOG_REPO"  vlog &&\
    cd vlog                      &&\
    git checkout "$VLOG_REF"     &&\
    mkdir build                  &&\
    cd build                     &&\
    cmake ..                     &&\
    make $MAKE_OPTS              &&\
    cp libvlog-core.so                    \
       trident/libtrident-core.so         \
       trident/libtrident-sparql.so       \
       trident/kognac/libkognac-core.so   \
       /usr/local/lib                   &&\
    cp vlog /usr/local/bin              &&\
    ldconfig                            &&\
    cd /tmp                       &&\
    git clone "$HDT_REPO" hdt-cpp &&\
    cd hdt-cpp                    &&\
    git checkout "$HDT_REF"       &&\
    ./autogen.sh                  &&\
    ./configure                   &&\
    make $HDT_MAKE_OPTS           &&\
    make install                  &&\
    ldconfig                      &&\
    rdf2hdt -V                    &&\
    cd /                                                    &&\
    rm -fr /tmp/*                                           &&\
    apt-get remove -y cmake g++ autoconf libtool pkg-config &&\
    apt-get remove -y zlib1g-dev libserd-dev                &&\
    apt-get clean -y

# Copy the freqel-server self-launching jar file
COPY freqel-server /usr/local/bin/

# >--- --- ---=[ Run-time configurations ]=--- --- ---<

# Options forwarded to the JVM (before -jar). This forwarding is implemented by the
# freqel-server launcher script
ENV JVM_ARGS=""

# >--- --- ---=[ Entrypoint ]=--- --- ---<
VOLUME /data
VOLUME /config
WORKDIR /data
ENTRYPOINT ["freqel-server"]
CMD ["--config", "/config/federation.yaml"]
