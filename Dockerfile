# this is our first build stage, it will not persist in the final image to ensure the ssh key is not in the final image
FROM ubuntu as intermediate

# install git
RUN apt-get update
RUN apt-get install -y \
        git \
        openssh-client

# Authorize SSH Host
RUN mkdir -p /root/.ssh && \
    chmod 0700 /root/.ssh
# See: https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/githubs-ssh-key-fingerprints
RUN echo "github.com ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl" > /root/.ssh/known_hosts
RUN echo "github.com ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBEmKSENjQEezOmxkZMy7opKgwFB9nkt5YRrYMjNuG5N87uRgg6CLrbo5wAdT/y6v0mKV0U2w0WZ2YB/++Tpockg=" > /root/.ssh/known_hosts
RUN echo "github.com ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCj7ndNxQowgcQnjshcLrqPEiiphnt+VTTvDP6mHBL9j1aNUkY4Ue1gvwnGLVlOhGeYrnZaMgRK6+PKCUXaDbC7qtbW8gIkhL7aGCsOr/C56SJMy/BCZfxd1nWzAOxSDPgVsmerOBYfNqltV9/hWCqBywINIR+5dIg6JTJ72pcEpEjcYgXkE2YEFXV1JHnsKgbLWNlhScqb2UmyRkQyytRLtL+38TGxkxCflmO+5Z8CSSNY7GidjMIZ7Q4zMjA2n1nGrlTDkzwDCsw+wqFPGQA179cnfGWOWRVruj16z6XyvxvjJwbz0wQZ75XK5tKSb7FNyeIEs4TT4jk+S4dhPeAUC5y+bDYirYgM4GC7uEnztnZyaVWQ7B381AK4Qdrwt51ZqExKbQpTUNn+EjqoTwvqNj4kqx5QUCI0ThS/YkOxJCXmPUWZbhjpCg56i+2aB6CmK2JGhn57K5mj0MNdBXA4/WnwH6XoPWJzK5Nyu2zB3nAZp+S5hpQs+p1vN1/wsjk=" > /root/.ssh/known_hosts

# Add the keys and set permissions
ARG SSH_PRIVATE_KEY
RUN echo "$SSH_PRIVATE_KEY" > /root/.ssh/id_rsa && \
    chmod 600 /root/.ssh/id_rsa

# make sure your domain is accepted
RUN touch /root/.ssh/known_hosts
# RUN ssh -T git@github.com

WORKDIR /root
RUN git clone git@github.com:OZGrav/meertime_ephemerides_and_templates.git


# this is our second build stage, it will be the final image
FROM ubuntu:22.04
COPY --from=intermediate /root/meertime_ephemerides_and_templates /tmp/meertime_ephemerides_and_templates

# Define home, psrhome, OSTYPE and create the directory
ENV HOME /home/psr
ENV PSRHOME $HOME/software
ENV OSTYPE linux
RUN mkdir -p $PSRHOME
WORKDIR $PSRHOME

# Install dependencies
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    add-apt-repository universe && \
    apt-get update && \
    apt-get --no-install-recommends -y install \
    build-essential \
    autoconf \
    autotools-dev \
    automake \
    autogen \
    libtool \
    pkg-config \
    cmake \
    csh \
    g++ \
    gcc \
    gfortran \
    wget \
    git \
    expect \
    libcfitsio-dev \
    hwloc \
    perl \
    pcre2-utils \
    libpcre2-dev \
    pgplot5 \
    python3.10 \
    python3-dev \
    python3-testresources \
    python3-pip \
    python3-setuptools \
    python3-tk \
    libfftw3-3 \
    libfftw3-bin \
    libfftw3-dev \
    libfftw3-single3 \
    libx11-dev \
    libpcre3 \
    libpcre3-dev \
    libpng-dev \
    libpnglite-dev \
    libhdf5-dev \
    libhdf5-serial-dev \
    libxml2 \
    libxml2-dev \
    libltdl-dev \
    libffi-dev \
    libssl-dev \
    libxft-dev \
    libfreetype6-dev \
    libblas-dev \
    liblapack-dev \
    libatlas-base-dev \
    gsl-bin \
    libgsl-dev \
    bc && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get -y clean

RUN pip install -U \
        pip \
        setuptools && \
    pip install -U \
        cython \
        scikit-image  \
        numpy && \
    pip install -U \
        pandas \
        matplotlib \
        astropy \
        scipy \
        psrdb==3.0.2 \
        git+https://github.com/danielreardon/MeerGuard \
        git+https://github.com/danielreardon/scintools.git


# setup environment variables

# general *PATH environment
ENV PATH $PATH:$PSRHOME/bin
ENV PYTHONPATH $PYTHONPATH:$PSRHOME/lib/python3.10/site-packages
# setup pgplot environment
ENV PGPLOT_DIR /usr
ENV PGPLOT_FONT $PGPLOT_DIR/lib/pgplot5/grfont.dat
ENV PGPLOT_INCLUDES $PGPLOT_DIR/include
ENV PGPLOT_BACKGROUND white
ENV PGPLOT_FOREGROUND black
ENV PGPLOT_DEV /xs
ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:$PGPLOT_DIR/lib
ENV C_INCLUDE_PATH $C_INCLUDE_PATH:$PGPLOT_DIR/include

# first get all repos then
RUN wget http://www.atnf.csiro.au/people/pulsar/psrcat/downloads/psrcat_pkg.tar.gz && tar -xvf psrcat_pkg.tar.gz -C $PSRHOME && rm psrcat_pkg.tar.gz && \
    wget https://www.imcce.fr/content/medias/recherche/equipes/asd/calceph/calceph-3.5.1.tar.gz && tar -xvf calceph-3.5.1.tar.gz -C $PSRHOME && rm calceph-3.5.1.tar.gz && \
    wget https://sourceforge.net/projects/swig/files/swig/swig-4.0.1/swig-4.0.1.tar.gz && tar -xvf swig-4.0.1.tar.gz -C $PSRHOME && rm swig-4.0.1.tar.gz && \
    git config --global http.postBuffer 1048576000 && \
    git clone git://git.code.sf.net/p/tempo/tempo tempo && \
    git clone https://bitbucket.org/psrsoft/tempo2.git && \
    git clone git://git.code.sf.net/p/psrchive/code psrchive && \
    git clone https://github.com/SixByNine/psrxml.git

# set swig environment
ENV SWIG_DIR $PSRHOME/swig
ENV SWIG_PATH $SWIG_DIR/bin
ENV PATH=$SWIG_PATH:$PATH
ENV SWIG_EXECUTABLE $SWIG_DIR/bin/swig
ENV SWIG $SWIG_EXECUTABLE
# build swig
RUN cd $PSRHOME/swig-4.0.1 && \
    ./configure --prefix=$SWIG_DIR && \
    make && \
    make install && \
    make clean

# set calceph environment
ENV CALCEPH $PSRHOME/calceph-3.5.1
ENV PATH $PATH:$CALCEPH/install/bin
ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:$CALCEPH/install/lib
ENV C_INCLUDE_PATH $C_INCLUDE_PATH:$CALCEPH/install/include
# build calceph
RUN cd $CALCEPH && \
    ./configure --prefix=$CALCEPH/install --with-pic --enable-shared --enable-static --enable-fortran --enable-thread && \
    make && \
    make check && \
    make install && \
    make clean

# set Psrcat environment
ENV PSRCAT_FILE $PSRHOME/psrcat_tar/psrcat.db
ENV PATH $PATH:$PSRHOME/psrcat_tar
# build psrcat
RUN cd $PSRHOME/psrcat_tar && \
    /bin/bash makeit

# set psrxml environment
ENV PSRXML $PSRHOME/psrxml
ENV PATH $PATH:$PSRXML/install/bin
ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:$PSRXML/install/lib
ENV C_INCLUDE_PATH $C_INCLUDE_PATH:$PSRXML/install/include
# build psrxml
RUN cd $PSRXML && \
    autoreconf --install --warnings=none && \
    ./configure --prefix=$PSRXML/install && \
    make && \
    make install && \
    make clean

# set tempo environment
ENV TEMPO_DIR $PSRHOME/tempo
ENV TEMPO $PSRHOME/tempo/install
ENV PATH $PATH:$TEMPO/bin
# tempo (and all it's little utilities)
RUN cd $TEMPO_DIR && \
    ./prepare && \
    ./configure --prefix=$TEMPO && \
    FFLAGS="$FFLAGS -O3 -m64" && \
    make -j && \
    make install && \
    # copy data files and build/install utilities
    cp -r clock/ ephem/ tzpar/ obsys.dat tempo.cfg tempo.hlp $TEMPO && \
    sed -i "s;${TEMPO_DIR};${TEMPO};g" ${TEMPO}/tempo.cfg && \
    cd ${TEMPO_DIR}/src && \
    make matrix && \
    cp matrix ${TEMPO}/bin/ && \
    cd ${TEMPO_DIR}/util/lk && \
    gfortran -o lk lk.f && \
    cp lk ${TEMPO}/bin/ && \
    cp ${TEMPO_DIR}/util/dmx/* ${TEMPO}/bin/ && \
    cp ${TEMPO_DIR}/util/dmxparse/* ${TEMPO}/bin/ && \
    cp ${TEMPO_DIR}/util/dmx_ranges/* ${TEMPO}/bin/ && \
    chmod +x ${TEMPO}/bin/DMX_ranges2.py && \
    cp ${TEMPO_DIR}/util/dmx_broaden/* ${TEMPO}/bin/ && \
    cp ${TEMPO_DIR}/util/cull/cull.pl ${TEMPO}/bin/cull && \
    cp ${TEMPO_DIR}/util/extract/extract.pl ${TEMPO}/bin/extract && \
    cp ${TEMPO_DIR}/util/obswgt/obswgt.pl ${TEMPO}//bin/obswg && \
    cd ${TEMPO_DIR}/util/print_resid && \
    make -j && \
    cp print_resid ${TEMPO}/bin/ && \
    cp ${TEMPO_DIR}/util/res_avg/* ${TEMPO}/bin/ && \
    cp ${TEMPO_DIR}/util/wgttpo/wgttpo.pl ${TEMPO}/bin/wgttpo && \
    cp ${TEMPO_DIR}/util/wgttpo/wgttpo_emin.pl ${TEMPO}/bin/wgttpo_emin && \
    cp ${TEMPO_DIR}/util/wgttpo/wgttpo_equad.pl ${TEMPO}/bin/wgttpo_equad && \
    cd ${TEMPO_DIR}/util/ut1 && \
    gcc -o predict_ut1 predict_ut1.c $(gsl-config --libs) && \
    cp predict_ut1 check.ut1 do.iers.ut1 do.iers.ut1.new get_ut1 get_ut1_new make_ut1 ${TEMPO}/bin/ && \
    cp ${TEMPO_DIR}/util/compare_tempo/compare_tempo ${TEMPO}/bin/ && \
    cp ${TEMPO_DIR}/util/pubpar/pubpar.py ${TEMPO}/bin/ && \
    chmod +x ${TEMPO}/bin/pubpar.py && \
    cp ${TEMPO_DIR}/util/center_epoch/center_epoch.py ${TEMPO}/bin/ && \
    cd ${TEMPO_DIR}/util/avtime && \
    gfortran -o avtime avtime.f && \
    cp avtime ${TEMPO}/bin/ && \
    cd ${TEMPO_DIR}/util/non_tempo && \
    cp dt mjd aolst ${TEMPO}/bin/ && \
    cd ${TEMPO_DIR}

# set tempo2 environment
ENV TEMPO2_DIR $PSRHOME/tempo2
ENV TEMPO2 $PSRHOME/tempo2/install
ENV TEMPO2_ALIAS tempo
ENV PATH $PATH:$TEMPO2/bin
ENV C_INCLUDE_PATH $C_INCLUDE_PATH:$TEMPO2/include
ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:$TEMPO2/lib
# tempo2
RUN cd $TEMPO2_DIR && \
    ./bootstrap && \
    cp -r T2runtime/ $TEMPO2/ && \
    ./configure --prefix=$TEMPO2 --with-x --x-libraries=/usr/lib/x86_64-linux-gnu --with-fftw3-dir=/usr/ --with-calceph=$CALCEPH/install/lib \
    --enable-shared --enable-static --with-pic \
    CPPFLAGS="-I"$CALCEPH"/install/include -L"$CALCEPH"/install/lib/ -I"$PGPLOT_DIR"/include/ -L"$PGPLOT_DIR"/lib/" \
    CXXFLAGS="-I"$CALCEPH"/install/include -L"$CALCEPH"/install/lib/ -I"$PGPLOT_DIR"/include/ -L"$PGPLOT_DIR"/lib/" && \
    make -j && \
    make -j plugins && \
    make install && \
    make plugins-install && \
    make clean && make plugins-clean

# set psrchive environment
ENV PSRCHIVE_DIR $PSRHOME/psrchive
ENV PSRCHIVE $PSRHOME/psrchive/install
ENV PATH $PATH:$PSRCHIVE/bin
ENV C_INCLUDE_PATH $C_INCLUDE_PATH:$PSRCHIVE/include
ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:$PSRCHIVE/lib
ENV PYTHONPATH $PYTHONPATH:$PSRCHIVE/lib/python3.10/site-packages
# psrchive (which requires tempo2 to be built)
RUN cd $PSRCHIVE_DIR && \
    ./bootstrap && \
    ./configure --prefix=$PSRCHIVE --with-x --x-libraries=/usr/lib/x86_64-linux-gnu --with-psrxml-dir=$PSRXML/install --enable-shared --enable-static \
    F77=gfortran \
    CPPFLAGS="-I"$CALCEPH"/install/include -L"$CALCEPH"/install/lib/ -I"$PGPLOT_DIR"/include/ -L"$PGPLOT_DIR"/lib/" \
    CXXFLAGS="-I"$CALCEPH"/install/include -L"$CALCEPH"/install/lib/ -I"$PGPLOT_DIR"/include/ -L"$PGPLOT_DIR"/lib/" \
    LDFLAGS="-L"$PSRXML"/install/lib -L"$CALCEPH"/install/lib/  -L"$PGPLOT_DIR"/lib/ " LIBS="-lpsrxml -lxml2" && \
    make -j && \
    make install && \
    make clean

# Private ephem_template repo already downloaded so now install it
WORKDIR /tmp/meertime_ephemerides_and_templates
RUN pip install .
RUN rm -rf /tmp/meertime_ephemerides_and_templates

# Finally install the actual repos software
COPY . $PSRHOME/meerpipe
WORKDIR $PSRHOME/meerpipe
RUN pip install .
