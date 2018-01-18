#!/bin/bash
set -x -e

# AWS EMR bootstrap script 
# for installing Jupyter notebook on AWS EMR 5+
#
# 2016-11-04 - Tom Zeng tomzeng@amazon.com, initial version
# 2016-11-20 - Tom Zeng, add JupyterHub
# 2016-12-01 - Tom Zeng, add s3 support and cached install
# 2016-12-03 - Tom Zeng, use puppet to install/run services
# 2016-12-06 - Tom Zeng, switch to s3fs for S3 support since s3nb is not fully working
# 2016-12-29 - Tom Zeng, add Dask and Dask.distributed
# 2017-04-18 - Tom Zeng, add BigDL support
# 2017-05-16 = Tom Zeng, add cached install for EMR 5.5, updated yum rpm cache and miniCRAN
# 2017-05-20 - Tom Zeng, add s3contents to replace s3nb which no longer works due to Jupyter update
# 2017-05-23 - Tom Zeng, fix the s3contents dummy last_modified field
# 2017-05-25 - Tom Zeng, turn off tensorflow, pip wheel install no longer working, will fix later
# 2017-06-09 - Tom Zeng, fix install issue for EMR 5.6 caused by kernel source package already installed
# 2017-11-29 - Tom Zeng, fix the broken JavaScript and CoffeeScript kernels, now works on EMR 5.10

#
# Usage:
# --r - install the IRKernel for R (Sparklyr is installed with this option, but as of 2017-04-05 Sparklyr does not support Spark 2.x yet)
# --toree - install the Apache Toree kernel that supports Scala, PySpark, SQL, SparkR for Apache Spark
# --interpreters - specify Apache Toree interpreters, default is all: "Scala,SQL,PySpark,SparkR"
# --julia - install the IJulia kernel for Julia
# --bigdl - install Intel's BigDL Deep Learning framework
# --ruby - install the iRuby kernel for Ruby
# --torch - intall the iTorch kernel for Torch
# --javascript - install the JavaScript and CoffeeScript kernels (only works for JupyterHub for now)
# --dask - install Dask and Dask.distributed, with the scheduler on master instance and the workers on the slave instances
# --ds-packages - install the Python Data Science related packages (scikit-learn pandas numpy numexpr statsmodels seaborn)
# --ml-packages - install the Python Machine Learning related packages (theano keras tensorflow)
# --python-packages - install specific python packages e.g. "ggplot nilean"
# --port - set the port for Jupyter notebook, default is 8888
# --user - create a default user for Jupyterhub
# --password - set the password for Jupyter notebook and JupyterHub
# --localhost-only - restrict jupyter to listen on localhost only, default to listen on all ip addresses for the instance
# --jupyterhub - install JupyterHub
# --jupyterhub-port - set the port for JupyterHub, default is 8000
# --no-jupyter - if JupyterHub is installed, use this to disable Jupyter
# --notebook-dir - specify notebook folder, this could be a local directory or a S3 bucket
# --cached-install - use some cached dependency artifacts on s3 to speed up installation
# --ssl - enable ssl, make sure to use your own cert and key files to get rid of the warning
# --copy-samples - copy sample notebooks to samples sub folder under the notebook folder
# --spark-opts - user supplied Spark options to pass to SPARK_OPTS
# --s3fs - use s3fs instead of s3contents(default) for storing notebooks on s3, s3fs could cause slowness if the s3 bucket has lots of file
# --python3 - install python 3 packages and use python3

# check for master node
IS_MASTER=false
if grep isMaster /mnt/var/lib/info/instance.json | grep true;
then
  IS_MASTER=true
fi

# error message
error_msg ()
{
  echo 1>&2 "Error: $1"
}

# some defaults
RUBY_KERNEL=false
R_KERNEL=false
JULIA_KERNEL=false
TOREE_KERNEL=false
TORCH_KERNEL=false
DS_PACKAGES=false
ML_PACKAGES=false
PYTHON_PACKAGES=""
RUN_AS_STEP=false
NOTEBOOK_DIR=""
NOTEBOOK_DIR_S3=false
JUPYTER_PORT=8888
JUPYTER_PASSWORD=""
JUPYTER_LOCALHOST_ONLY=false
PYTHON3=false
GPU=false
CPU_GPU="cpu"
GPUU=""
JUPYTER_HUB=true
JUPYTER_HUB_PORT=8000
JUPYTER_HUB_IP="*"
JUPYTER_HUB_DEFAULT_USER="jupyter"
INTERPRETERS="Scala,SQL,PySpark,SparkR"
R_REPOS_LOCAL="file:////mnt/miniCRAN"
R_REPOS_REMOTE="http://cran.rstudio.com"
USE_CACHED_DEPS=false
R_REPOS=$R_REPOS_REMOTE
SSL=false
SSL_OPTS="--no-ssl"
COPY_SAMPES=false
USER_SPARK_OPTS=""
NOTEBOOK_DIR_S3_S3NB=false
NOTEBOOK_DIR_S3_S3CONTENTS=true
JS_KERNEL=false
NO_JUPYTER=false
INSTALL_DASK=false
INSTALL_PY3_PKGS=false
APACHE_SPARK_VERSION="2.2.0"
BIGDL=false
MXNET=false
DL4J=false
NPROC=$(nproc)

# get input parameters
while [ $# -gt 0 ]; do
    case "$1" in
    --r)
      R_KERNEL=true
      ;;
    --julia)
      JULIA_KERNEL=true
      ;;
    --toree)
      TOREE_KERNEL=true
      ;;
    --torch)
      TORCH_KERNEL=true
      ;;
    --javascript)
      JS_KERNEL=true
      ;;
    --ds-packages)
      DS_PACKAGES=true
      ;;
    --ml-packages)
      ML_PACKAGES=true
      ;;
    --python-packages)
      shift
      PYTHON_PACKAGES=$1
      ;;
    --bigdl)
      BIGDL=true
      ;;
    --mxnet)
      MXNET=true
      ;;
    --dl4j)
      DL4J=true
      ;;
    --ruby)
      RUBY_KERNEL=true
      ;;
    --gpu)
      GPU=true
      CPU_GPU="gpu"
      GPUU="_gpu"
      ;;
    --run-as-step)
      RUN_AS_STEP=true
      ;;
    --port)
      shift
      JUPYTER_PORT=$1
      ;;
    --user)
      shift
      JUPYTER_HUB_DEFAULT_USER=$1
      ;;
    --password)
      shift
      JUPYTER_PASSWORD=$1
      ;;
    --localhost-only)
      JUPYTER_LOCALHOST_ONLY=true
      JUPYTER_HUB_IP=""
      ;;
    --jupyterhub)
      JUPYTER_HUB=true
      #PYTHON3=true
      ;;
    --jupyterhub-port)
      shift
      JUPYTER_HUB_PORT=$1
      ;;
    --notebook-dir)
      shift
      NOTEBOOK_DIR=$1
      ;;
    --copy-samples)
      COPY_SAMPLES=true
      ;;
    --toree-interpreters)
      shift
      INTERPRETERS=$1
      ;;
    --cached-install)
      #USE_CACHED_DEPS=true
      #R_REPOS=$R_REPOS_LOCAL
      ;;
    --no-cached-install)
      USE_CACHED_DEPS=false
      R_REPOS=$R_REPOS_REMOTE
      ;;
    --no-jupyter)
      NO_JUPYTER=true
      ;;
    --ssl)
      SSL=true
      ;;
    --dask)
      INSTALL_DASK=true
      ;;
    --python3)
      INSTALL_PY3_PKGS=true
      ;;
    --spark-opts)
      shift
      USER_SPARK_OPTS=$1
      ;;
    --spark-version)
      shift
      APACHE_SPARK_VERSION=$1
      ;;
    --s3fs)
      #NOTEBOOK_DIR_S3_S3NB=false
      NOTEBOOK_DIR_S3_S3CONTENTS=false
      ;;
    #--s3nb) # this stopped working after Jupyter update in early 2017
    #  NOTEBOOK_DIR_S3_S3NB=true
    #  ;;
    -*)
      # do not exit out, just note failure
      error_msg "unrecognized option: $1"
      ;;
    *)
      break;
      ;;
    esac
    shift
done

sudo bash -c 'echo "fs.file-max = 25129162" >> /etc/sysctl.conf'
sudo sysctl -p /etc/sysctl.conf
sudo bash -c 'echo "* soft    nofile          1048576" >> /etc/security/limits.conf'
sudo bash -c 'echo "* hard    nofile          1048576" >> /etc/security/limits.conf'
sudo bash -c 'echo "session    required   pam_limits.so" >> /etc/pam.d/su'

sudo puppet module install spantree-upstart

RELEASE=$(cat /etc/system-release)
REL_NUM=$(ruby -e "puts '$RELEASE'.split.last")

if [ "$USE_CACHED_DEPS" = true ]; then
  cd /mnt
  aws s3 cp s3://aws-bigdata-blog/artifacts/aws-blog-emr-jupyter/jupyter-deps.zip .
  aws s3 cp s3://aws-bigdata-blog/artifacts/aws-blog-emr-jupyter/yum-rpm-cache-$REL_NUM.zip .
  unzip jupyter-deps.zip
  unzip yum-rpm-cache-$REL_NUM.zip
  rm jupyter-deps.zip
  rm yum-rpm-cache-$REL_NUM.zip
fi

if [ "$R_REPOS" = "$R_REPOS_LOCAL" ]; then
  cd /mnt
  #aws s3 cp s3://aws-bigdata-blog/artifacts/aws-blog-emr-jupyter/miniCRAN-20170516.zip miniCRAN.zip
  aws s3 cp s3://aws-bigdata-blog/artifacts/aws-blog-emr-jupyter/miniCRAN.zip miniCRAN.zip
  unzip miniCRAN.zip
  rm miniCRAN.zip
fi

sudo mkdir -p /mnt/var/aws/emr
sudo cp -pr /var/aws/emr/packages /mnt/var/aws/emr/ && sudo rm -rf /var/aws/emr/packages && sudo mkdir /var/aws/emr/packages && sudo mount -o bind /mnt/var/aws/emr/packages /var/aws/emr/packages &

# move /usr/local and usr/share to /mnt/usr-moved/ to avoid running out of space on /
if [ ! -d /mnt/usr-moved ]; then
  echo "move local start" >> /tmp/install_time.log
  date >> /tmp/install_time.log
  sudo mkdir /mnt/usr-moved
  sudo mv /usr/local /mnt/usr-moved/ && sudo ln -s /mnt/usr-moved/local /usr/
  echo "move local end, move share start" >> /tmp/install_time.log
  date >> /tmp/install_time.log
  sudo mv /usr/share /mnt/usr-moved/ && sudo ln -s /mnt/usr-moved/share /usr/
  echo "move share end" >> /tmp/install_time.log
  date >> /tmp/install_time.log
fi

export MAKE="make -j $NPROC"

sudo yum remove -y kernel-4.9.27-14.31.amzn1.x86_64 # EMR 5.6
if [ "$USE_CACHED_DEPS" = true ]; then
  sudo yum install -y libcurl-devel # workaround for EMR 5.9 until libcurl cache is fixed
  sudo yum install --skip-broken -y /mnt/yum-rpm-cache/*
else
  sudo yum install -y xorg-x11-xauth.x86_64 xorg-x11-server-utils.x86_64 xterm libXt libX11-devel libXt-devel libcurl libcurl-devel git graphviz cyrus-sasl cyrus-sasl-devel readline readline-devel gnuplot
  sudo yum install --enablerepo=epel -y nodejs npm zeromq3 zeromq3-devel
  sudo yum install -y gcc-c++ patch zlib zlib-devel
  sudo  yum install -y libyaml-devel libffi-devel openssl-devel make
  sudo yum install -y bzip2 autoconf automake libtool bison iconv-devel sqlite-devel
fi

cd /mnt
PYTHON3=false
if [ "$PYTHON3" = true ]; then # this will break bigtop/puppet which relies on python 2, so disable with the line above
  export PYSPARK_PYTHON="python3"
  sudo python3 -m pip install --upgrade pip
  if [ -f /usr/bin/python3.4 ]; then
    sudo ln -sf /usr/bin/python3.4 /usr/bin/python
  fi
  if [ -f /usr/bin/pip-3.4 ]; then
    sudo ln -sf /usr/bin/pip-3.4 /usr/bin/pip
  fi
else
  sudo python -m pip install --upgrade pip
  if [ -f /usr/bin/pip-2.7 ]; then
    sudo ln -sf /usr/bin/pip-2.7 /usr/bin/pip
  fi
fi

export NODE_PATH='/usr/lib/node_modules'
if [ "$JS_KERNEL" = true ]; then
  sudo python -m pip install jinja2 tornado jsonschema pyzmq
  # the following no longer working or needed on the latest EMR version 5.10
  #sudo npm cache clean -f
  #sudo npm install -g npm
  #sudo npm install -g n
  #sudo n stable
fi

TF_BINARY_URL_PY3="https://storage.googleapis.com/tensorflow/linux/$CPU_GPU/tensorflow$GPUU-1.4.0-cp34-cp34m-linux_x86_64.whl"
TF_BINARY_URL="https://storage.googleapis.com/tensorflow/linux/$CPU_GPU/tensorflow$GPUU-1.4.0-cp27-none-linux_x86_64.whl"

sudo python3 -m pip install jupyter
sudo ln -sf /usr/local/bin/ipython /usr/bin/
sudo ln -sf /usr/local/bin/jupyter /usr/bin/
if [ "$INSTALL_PY3_PKGS" = true ]; then
  sudo python3 -m pip install matplotlib seaborn bokeh cython networkx findspark
  sudo python3 -m pip install mrjob pyhive sasl thrift thrift-sasl snakebite
else
  sudo python -m pip install matplotlib seaborn bokeh cython networkx findspark
  sudo python -m pip install mrjob pyhive sasl thrift thrift-sasl snakebite
fi

if [ "$DS_PACKAGES" = true ]; then
  # Python
  if [ "$INSTALL_PY3_PKGS" = true ]; then
    sudo python3 -m pip install scikit-learn pandas numpy numexpr statsmodels scipy
  else
    sudo python -m pip install scikit-learn pandas numpy numexpr statsmodels scipy    
  fi
  # Javascript
  if [ "$JS_KERNEL" = true ]; then
    sudo npm install -g --unsafe-perm stats-analysis decision-tree machine_learning limdu synaptic node-svm lda brain.js scikit-node
  fi
fi

if [ "$ML_PACKAGES" = true ]; then
  if [ "$INSTALL_PY3_PKGS" = true ]; then
    sudo python3 -m pip install mxnet==0.11.0
    sudo python3 -m pip install $TF_BINARY_URL_PY3
    sudo python3 -m pip install theano
    sudo python3 -m pip install keras
    sudo python3 -m pip install xgboost lightgbm opencv-python
  else
    sudo python -m pip install mxnet==0.11.0
    sudo python -m pip install $TF_BINARY_URL
    sudo python -m pip install theano
    sudo python -m pip install keras
    sudo python -m pip install xgboost lightgbm opencv-python
  fi
fi

if [ ! "$PYTHON_PACKAGES" = "" ]; then
  if [ "$INSTALL_PY3_PKGS" = true ]; then
    sudo python3 -m pip install $PYTHON_PACKAGES || true
  else
    sudo python -m pip install $PYTHON_PACKAGES || true
  fi
fi

if [ "$BIGDL" = true ]; then
  aws s3 cp s3://tomzeng/maven/apache-maven-3.3.3-bin.tar.gz .
  tar xvfz apache-maven-3.3.3-bin.tar.gz
  sudo mv apache-maven-3.3.3 /opt/maven
  sudo ln -s /opt/maven/bin/mvn /usr/bin/mvn

  git clone https://github.com/intel-analytics/BigDL.git
  cd BigDL/
  export MAVEN_OPTS="-Xmx2g -XX:ReservedCodeCacheSize=512m"
  export BIGDL_HOME=/mnt/BigDL
  export BIGDL_VER="0.4.0-SNAPSHOT"
  bash make-dist.sh -P spark_2.2
  mkdir /tmp/bigdl_summaries
  /usr/local/bin/tensorboard --debug INFO --logdir /tmp/bigdl_summaries/ > /tmp/tensorboard_bigdl.log 2>&1 &
fi

if [ "$JULIA_KERNEL" = true ]; then
  # Julia install
  cd /mnt
  if [ ! "$USE_CACHED_DEPS" = true ]; then
    wget https://julialang.s3.amazonaws.com/bin/linux/x64/0.5/julia-0.5.0-linux-x86_64.tar.gz
    tar xvfz julia-0.5.0-linux-x86_64.tar.gz
    #wget https://julialang-s3.julialang.org/bin/linux/x64/0.6/julia-0.6.1-linux-x86_64.tar.gz
    #tar xvfz julia-0.6.1-linux-x86_64.tar.gz
  fi
  cd julia-3c9d75391c
  #cd julia-0d7248e2ff
  sudo cp -pr bin/* /usr/bin/
  sudo cp -pr lib/* /usr/lib/
  #sudo cp -pr libexec/* /usr/libexec/
  sudo cp -pr share/* /usr/share/
  sudo cp -pr include/* /usr/include/
fi

if [ "$INSTALL_DASK" = true ]; then
  if [ "$INSTALL_PY3_PKGS" = true ]; then
    sudo python3 -m pip install dask[complete] distributed
  else
    sudo python -m pip install dask[complete] distributed
  fi
  export PATH=$PATH:/usr/local/bin
  if [ "$IS_MASTER" = true ]; then
    dask-scheduler > /var/log/dask-scheduler.log 2>&1 &
  else
    MASTER_KV=$(grep masterHost /emr/instance-controller/lib/info/job-flow-state.txt)
    MASTER_HOST=$(ruby -e "puts '$MASTER_KV'.gsub('\"','').split.last")
    dask-worker $MASTER_HOST:8786 > /var/log/dask-worker.log 2>&1 &
  fi
fi

#echo ". /mnt/ipython-env/bin/activate" >> ~/.bashrc

# only run below on master instance
if [ "$IS_MASTER" = true ]; then

if [ "$RUBY_KERNEL" = true ]; then
  cd /mnt
  if [ "$USE_CACHED_DEPS" != true ]; then
    wget http://ftp.ruby-lang.org/pub/ruby/2.1/ruby-2.1.8.tar.gz 
    tar xvzf ruby-2.1.8.tar.gz
  fi
  cd ruby-2.1.8
  ./configure --prefix=/usr
  make -j $NPROC
  sudo make install
  #sudo gem install puppet -v=3.7.4 -N
  sudo gem install rbczmq iruby -N
  sudo gem install presto-client -N
  sudo iruby register
  sudo cp -pr /root/.ipython/kernels/ruby /usr/local/share/jupyter/kernels/
  iruby register
fi

sudo mkdir -p /var/log/jupyter
mkdir -p ~/.jupyter
touch ls ~/.jupyter/jupyter_notebook_config.py

sed -i '/c.NotebookApp.open_browser/d' ~/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.open_browser = False" >> ~/.jupyter/jupyter_notebook_config.py

if [ ! "$JUPYTER_LOCALHOST_ONLY" = true ]; then
sed -i '/c.NotebookApp.ip/d' ~/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.ip='*'" >> ~/.jupyter/jupyter_notebook_config.py
fi

sed -i '/c.NotebookApp.port/d' ~/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.port = $JUPYTER_PORT" >> ~/.jupyter/jupyter_notebook_config.py

if [ ! "$JUPYTER_PASSWORD" = "" ]; then
  sed -i '/c.NotebookApp.password/d' ~/.jupyter/jupyter_notebook_config.py
  HASHED_PASSWORD=$(python3 -c "from notebook.auth import passwd; print(passwd('$JUPYTER_PASSWORD'))")
  echo "c.NotebookApp.password = u'$HASHED_PASSWORD'" >> ~/.jupyter/jupyter_notebook_config.py
else
  sed -i '/c.NotebookApp.token/d' ~/.jupyter/jupyter_notebook_config.py
  echo "c.NotebookApp.token = u''" >> ~/.jupyter/jupyter_notebook_config.py
fi

echo "c.Authenticator.admin_users = {'$JUPYTER_HUB_DEFAULT_USER'}" >> ~/.jupyter/jupyter_notebook_config.py
echo "c.LocalAuthenticator.create_system_users = True" >> ~/.jupyter/jupyter_notebook_config.py

if [ "$SSL" = true ]; then
  #NOTE - replace server.cert and server.key with your own cert and key files
  CERT=/usr/local/etc/server.cert
  KEY=/usr/local/etc/server.key
  sudo openssl req -x509 -nodes -days 3650 -newkey rsa:1024 -keyout $KEY -out $CERT -subj "/C=US/ST=Washington/L=Seattle/O=JupyterCert/CN=JupyterCert"
  
  # the following works for Jupyter but will fail JupyterHub, use options for both instead
  #echo "c.NotebookApp.certfile = u'/usr/local/etc/server.cert'" >> ~/.jupyter/jupyter_notebook_config.py
  #echo "c.NotebookApp.keyfile = u'/usr/local/etc/server.key'" >> ~/.jupyter/jupyter_notebook_config.py

  SSL_OPTS_JUPYTER="--keyfile=/usr/local/etc/server.key --certfile=/usr/local/etc/server.cert"
  SSL_OPTS_JUPYTERHUB="--ssl-key=/usr/local/etc/server.key --ssl-cert=/usr/local/etc/server.cert"
fi

# install default kernels
sudo python3 -m pip install notebook ipykernel
sudo python3 -m ipykernel install
sudo python -m pip install notebook ipykernel
sudo python -m ipykernel install
sudo python3 -m pip install metakernel
#sudo python3 -m pip install gnuplot_kernel
#sudo python3 -m gnuplot_kernel install
sudo python3 -m pip install bash_kernel
sudo python3 -m bash_kernel.install

# Javascript/CoffeeScript kernels
if [ "$JS_KERNEL" = true ]; then
  sudo npm install -g --unsafe-perm ijavascript d3 lodash plotly jp-coffeescript
  sudo ijs --ijs-install=global
  sudo jp-coffee --jp-install=global
fi
sudo python3 -m pip install jupyter_contrib_nbextensions
sudo python -m pip install jupyter_contrib_nbextensions
sudo jupyter contrib nbextension install --system
sudo python3 -m pip install jupyter_nbextensions_configurator
sudo python -m pip install jupyter_nbextensions_configurator
sudo jupyter nbextensions_configurator enable --system
sudo python3 -m pip install ipywidgets
sudo python -m pip install ipywidgets
sudo jupyter nbextension enable --py --sys-prefix widgetsnbextension

sudo python3 -m pip install pyeda # only work for python3
sudo python -m pip install gvmagic py_d3
aws s3 cp s3://aws-bigdata-blog/artifacts/aws-blog-emr-jupyter/rpy2-2.8.6.tar.gz /mnt/
sudo python -m pip install /mnt/rpy2-2.8.6.tar.gz
sudo python -m pip install ipython-sql

if [ "$JULIA_KERNEL" = true ]; then
  julia -e 'Pkg.add("IJulia")'
  julia -e 'Pkg.add("RDatasets");Pkg.add("Gadfly");Pkg.add("DataFrames");Pkg.add("PyPlot")'
  # Julia's Spark support does not support Spark on Yarn yet
  # install mvn
  #cd /mnt
  #aws s3 cp s3://tomzeng/maven/apache-maven-3.3.9-bin.tar.gz .
  #tar xvfz apache-maven-3.3.9-bin.tar.gz
  #sudo mv apache-maven-3.3.9 /opt/maven
  #sudo ln -s /opt/maven/bin/mvn /usr/bin/mvn
  # install Spark for Julia
  #julia -e 'Pkg.clone("https://github.com/dfdx/Spark.jl"); Pkg.build("Spark"); Pkg.checkout("JavaCall")'
fi

# iTorch depends on Torch which is installed with --ml-packages
if [ "$TORCH_KERNEL" = true ]; then
  set +e # workaround for the lengthy torch install-deps, esp when other background process are also running yum
  cd /mnt
  if [ ! "$USE_CACHED_DEPS" = true ]; then
    git clone https://github.com/torch/distro.git torch-distro
  fi
  cd torch-distro
  git pull
  ./install-deps
  ./install.sh -b
  export PATH=$PATH:/mnt/torch-distro/install/bin
  source ~/.profile
  luarocks install lzmq
  luarocks install gnuplot
  cd /mnt
  if [ ! "$USE_CACHED_DEPS" = true ]; then
    git clone https://github.com/facebook/iTorch.git
  fi
  cd iTorch
  luarocks make
  sudo cp -pr ~/.ipython/kernels/itorch /usr/local/share/jupyter/kernels/
  set -e
fi

if [ "$R_KERNEL" = true ] || [ "$TOREE_KERNEL" = true ]; then
  if [ ! -f /tmp/Renvextra ]; then # check if the rstudio ba was run, it does this already 
   sudo sed -i "s/make/make -j $NPROC/g" /usr/lib64/R/etc/Renviron
  fi

  sudo R --no-save << R_SCRIPT
    #install.packages(c('devtools', 'RJSONIO', 'itertools', 'digest', 'Rcpp', 'functional', 'httr', 'plyr', 'stringr', 'reshape2', 'caTools', 'rJava', 'DBI', 'ggplot2', 'dplyr', 'R.methodsS3', 'Hmisc', 'memoise', 'rjson'), repos="$R_REPOS", quiet = FALSE)
    install.packages(c('devtools'), repos="$R_REPOS", quiet = FALSE)
    # For the Granger Causuality example deps
    library(devtools)
    #install.packages(c('fUnitRoots','vars','aod','tseries'), repos="$R_REPOS", quiet = FALSE)
R_SCRIPT
fi

# IRKernal setup 
if [ "$R_KERNEL" = true ]; then
  sudo R --no-save << R_SCRIPT
    install.packages(c("curl", "httr", "repr", "IRdisplay", "evaluate", "crayon", "pbdZMQ", "uuid", "digest", "e1071", "party"), repos="$R_REPOS", quiet = TRUE)
    devtools::install_github("IRkernel/IRkernel")
    IRkernel::installspec(user = FALSE)
R_SCRIPT
fi

if [ ! "$NOTEBOOK_DIR" = "" ]; then
  NOTEBOOK_DIR="${NOTEBOOK_DIR%/}/" # remove trailing / if exists then add /
  if [[ "$NOTEBOOK_DIR" == s3://* ]]; then
    NOTEBOOK_DIR_S3=true
    # the s3nb does not fully working yet(upload and createe folder not working)
    # s3nb does not work anymore due to Jupyter update
    if [ "$NOTEBOOK_DIR_S3_S3NB" = true ]; then
      cd /mnt
      if [ ! "$USE_CACHED_DEPS" = true ]; then
        git clone https://github.com/tomz/s3nb.git
      fi
      cd s3nb
      sudo python -m pip install entrypoints
      sudo python setup.py install
      if [ "$JUPYTER_HUB" = true ]; then
        sudo python3 -m pip install entrypoints
        sudo python3 setup.py install
      fi

      echo "c.NotebookApp.contents_manager_class = 's3nb.S3ContentsManager'" >> ~/.jupyter/jupyter_notebook_config.py
      echo "c.S3ContentsManager.checkpoints_kwargs = {'root_dir': '~/.checkpoints'}" >> ~/.jupyter/jupyter_notebook_config.py
      # if just bucket with no subfolder, a trailing / is required, otherwise s3nb will break
      echo "c.S3ContentsManager.s3_base_uri = '$NOTEBOOK_DIR'" >> ~/.jupyter/jupyter_notebook_config.py
      #echo "c.S3ContentsManager.s3_base_uri = '${NOTEBOOK_DIR_S3%/}/%U'" >> ~/.jupyter/jupyter_notebook_config.py
      #echo "c.Spawner.default_url = '${NOTEBOOK_DIR_S3%/}/%U'" >> ~/.jupyter/jupyter_notebook_config.py
      #echo "c.Spawner.notebook_dir = '/%U'" >> ~/.jupyter/jupyter_notebook_config.py 
    elif [ "$NOTEBOOK_DIR_S3_S3CONTENTS" = true ]; then
      BUCKET=$(ruby -e "puts '$NOTEBOOK_DIR'.split('//')[1].split('/')[0]")
      FOLDER=$(ruby -e "puts '$NOTEBOOK_DIR'.split('//')[1].split('/')[1..-1].join('/')")
      #sudo python -m pip install s3contents
      cd /mnt
      #aws s3 cp s3://aws-bigdata-blog/artifacts/aws-blog-emr-jupyter/s3contents.zip .
      #unzip s3contents.zip
      git clone https://github.com/tomz/s3contents.git
      cd s3contents
      sudo python setup.py install
      echo "c.NotebookApp.contents_manager_class = 's3contents.S3ContentsManager'" >> ~/.jupyter/jupyter_notebook_config.py
      echo "c.S3ContentsManager.bucket_name = '$BUCKET'" >> ~/.jupyter/jupyter_notebook_config.py
      echo "c.S3ContentsManager.prefix = '$FOLDER'" >> ~/.jupyter/jupyter_notebook_config.py
      # this following is no longer needed, default was fixed in the latest on github
      #echo "c.S3ContentsManager.endpoint_url = 'https://s3.amazonaws.com'" >> ~/.jupyter/jupyter_notebook_config.py
    else
      BUCKET=$(ruby -e "puts '$NOTEBOOK_DIR'.split('//')[1].split('/')[0]")
      FOLDER=$(ruby -e "puts '$NOTEBOOK_DIR'.split('//')[1].split('/')[1..-1].join('/')")
      if [ "$USE_CACHED_DEPS" != true ]; then
        sudo yum install -y automake fuse fuse-devel libxml2-devel
      fi
      cd /mnt
      git clone https://github.com/s3fs-fuse/s3fs-fuse.git
      cd s3fs-fuse/
      ls -alrt
      ./autogen.sh
      ./configure
      make -j $NPROC
      sudo make install
      sudo su -c 'echo user_allow_other >> /etc/fuse.conf'
      mkdir -p /mnt/s3fs-cache
      mkdir -p /mnt/$BUCKET
      #/usr/local/bin/s3fs -o allow_other -o iam_role=auto -o umask=0 $BUCKET /mnt/$BUCKET
      # -o nodnscache -o nosscache -o parallel_count=20  -o multipart_size=50
      /usr/local/bin/s3fs -o allow_other -o iam_role=auto -o umask=0 -o url=https://s3.amazonaws.com  -o no_check_certificate -o enable_noobj_cache -o use_cache=/mnt/s3fs-cache $BUCKET /mnt/$BUCKET
      #/usr/local/bin/s3fs -o allow_other -o iam_role=auto -o umask=0 -o use_cache=/mnt/s3fs-cache $BUCKET /mnt/$BUCKET
      echo "c.NotebookApp.notebook_dir = '/mnt/$BUCKET/$FOLDER'" >> ~/.jupyter/jupyter_notebook_config.py
      echo "c.ContentsManager.checkpoints_kwargs = {'root_dir': '.checkpoints'}" >> ~/.jupyter/jupyter_notebook_config.py
    fi
  else
    echo "c.NotebookApp.notebook_dir = '$NOTEBOOK_DIR'" >> ~/.jupyter/jupyter_notebook_config.py
    echo "c.ContentsManager.checkpoints_kwargs = {'root_dir': '.checkpoints'}" >> ~/.jupyter/jupyter_notebook_config.py
  fi
fi

if [ ! "$JUPYTER_HUB_DEFAULT_USER" = "" ]; then
  sudo adduser $JUPYTER_HUB_DEFAULT_USER
fi

if [ "$COPY_SAMPLES" = true ]; then
  cd ~
  if [ "$NOTEBOOK_DIR_S3" = true ]; then
    aws s3 sync s3://aws-bigdata-blog/artifacts/aws-blog-emr-jupyter/notebooks/ ${NOTEBOOK_DIR}samples/ || true
  else
    if [ ! "$NOTEBOOK_DIR" = "" ]; then
      mkdir -p ${NOTEBOOK_DIR}samples || true
      sudo mkdir /home/$JUPYTER_HUB_DEFAULT_USER/${NOTEBOOK_DIR}samples || true
    fi
    aws s3 sync s3://aws-bigdata-blog/artifacts/aws-blog-emr-jupyter/notebooks/ ${NOTEBOOK_DIR}samples || true
    sudo cp -pr ${NOTEBOOK_DIR}samples /home/$JUPYTER_HUB_DEFAULT_USER/
    sudo chown -R $JUPYTER_HUB_DEFAULT_USER:$JUPYTER_HUB_DEFAULT_USER /home/$JUPYTER_HUB_DEFAULT_USER/${NOTEBOOK_DIR}samples
  fi
  if [ "$BIGDL" = true ]; then
    aws s3 cp s3://aws-bigdata-blog/artifacts/aws-blog-emr-jupyter/notebooks/text_classfication.ipynb ${NOTEBOOK_DIR}.
    sudo cp ${NOTEBOOK_DIR}text_classfication.ipynb /home/$JUPYTER_HUB_DEFAULT_USER/${NOTEBOOK_DIR}
    sudo chown -R $JUPYTER_HUB_DEFAULT_USER:$JUPYTER_HUB_DEFAULT_USER /home/$JUPYTER_HUB_DEFAULT_USER/${NOTEBOOK_DIR}text_classfication.ipynb
  fi
fi


wait_for_spark() {
  while [ ! -f /var/run/spark/spark-history-server.pid ]
  do
    sleep 10
  done
}

setup_jupyter_process_with_bigdl() {
  wait_for_spark
  export PYTHON_API_PATH=${BIGDL_HOME}/dist/lib/bigdl-$BIGDL_VER-python-api.zip
  export BIGDL_JAR_PATH=${BIGDL_HOME}/dist/lib/bigdl-$BIGDL_VER-jar-with-dependencies.jar
  cat ${BIGDL_HOME}/dist/conf/spark-bigdl.conf | sudo tee -a /etc/spark/conf/spark-defaults.conf
  sudo puppet apply << PUPPET_SCRIPT
  include 'upstart'
  upstart::job { 'jupyter':
    description    => 'Jupyter',
    respawn        => true,
    respawn_limit  => '0 10',
    start_on       => 'runlevel [2345]',
    stop_on        => 'runlevel [016]',
    console        => 'output',
    chdir          => '/home/hadoop',
    script           => '
    sudo su - hadoop > /var/log/jupyter/jupyter.log 2>&1 <<BASH_SCRIPT
    export NODE_PATH="$NODE_PATH"
    export PYSPARK_DRIVER_PYTHON="jupyter"
    export PYSPARK_DRIVER_PYTHON_OPTS="notebook --no-browser $SSL_OPTS_JUPYTER --log-level=INFO"
    export NOTEBOOK_DIR="$NOTEBOOK_DIR"

    export BIGDL_HOME=/mnt/BigDL
    export SPARK_HOME=/usr/lib/spark
    export YARN_CONF_DIR=/etc/hadoop/conf
    export PYTHONPATH=${PYTHON_API_PATH}:$PYTHONPATH
    source ${BIGDL_HOME}/dist/bin/bigdl.sh
    #pyspark --py-files ${PYTHON_API_PATH} --jars ${BIGDL_JAR_PATH} --conf spark.driver.extraClassPath=${BIGDL_JAR_PATH} --conf spark.executor.extraClassPath=bigdl-${BIGDL_VER}-jar-with-dependencies.jar
    pyspark --py-files ${PYTHON_API_PATH} --jars ${BIGDL_JAR_PATH}
BASH_SCRIPT
    ',
  }
PUPPET_SCRIPT
}

background_install_proc() {
  wait_for_spark
  
  if ! grep "spark.sql.catalogImplementation" /etc/spark/conf/spark-defaults.conf; then
    sudo bash -c "echo 'spark.sql.catalogImplementation  hive' >> /etc/spark/conf/spark-defaults.conf"
  fi
  
  if [ ! -f /tmp/Renvextra ]; then # check if the rstudio BA maybe already done this
    cat << 'EOF' > /tmp/Renvextra
JAVA_HOME="/etc/alternatives/jre"
HADOOP_HOME_WARN_SUPPRESS="true"
HADOOP_HOME="/usr/lib/hadoop"
HADOOP_PREFIX="/usr/lib/hadoop"
HADOOP_MAPRED_HOME="/usr/lib/hadoop-mapreduce"
HADOOP_YARN_HOME="/usr/lib/hadoop-yarn"
HADOOP_COMMON_HOME="/usr/lib/hadoop"
HADOOP_HDFS_HOME="/usr/lib/hadoop-hdfs"
HADOOP_CONF_DIR="/usr/lib/hadoop/etc/hadoop"
YARN_CONF_DIR="/usr/lib/hadoop/etc/hadoop"
YARN_HOME="/usr/lib/hadoop-yarn"
HIVE_HOME="/usr/lib/hive"
HIVE_CONF_DIR="/usr/lib/hive/conf"
HBASE_HOME="/usr/lib/hbase"
HBASE_CONF_DIR="/usr/lib/hbase/conf"
SPARK_HOME="/usr/lib/spark"
SPARK_CONF_DIR="/usr/lib/spark/conf"
PATH=${PWD}:${PATH}
EOF

  #if [ "$PYSPARK_PYTHON" = "python3" ]; then
  if [ "$INSTALL_PY3_PKGS" = true ]; then
    cat << 'EOF' >> /tmp/Renvextra
PYSPARK_PYTHON="python3"
EOF
  fi

  cat /tmp/Renvextra | sudo tee -a /usr/lib64/R/etc/Renviron

  sudo mkdir -p /mnt/spark
  sudo chmod a+rwx /mnt/spark
  if [ -d /mnt1 ]; then
    sudo mkdir -p /mnt1/spark
    sudo chmod a+rwx /mnt1/spark
  fi

  sudo R --no-save << R_SCRIPT
  library(devtools)
  devtools::install_github("rstudio/sparklyr")
  install.packages(c('nycflights13', 'Lahman', 'data.table'), repos="$R_REPOS", quiet = TRUE)
R_SCRIPT

  set +e # workaround for if SparkR is already installed by other BA
  # install SparkR and SparklyR for R - toree ifself does not need this
  sudo R --no-save << R_SCRIPT
  library(devtools)
  install('/usr/lib/spark/R/lib/SparkR')
R_SCRIPT
  set -e

  fi # end if -f /tmp/Renvextra

  if [ "$INSTALL_PY3_PKGS" = true ]; then
    sudo python3 -m pip install /mnt/incubator-toree/dist/toree-pip
  else
    sudo python -m pip install /mnt/incubator-toree/dist/toree-pip
  fi
  #sudo ln -sf /usr/local/bin/jupyter-toree /usr/bin/
  export SPARK_HOME="/usr/lib/spark"
  SPARK_PACKAGES=""

  PYSPARK_PYTHON="python"
  if [ "$INSTALL_PY3_PKGS" = true ]; then
    PYSPARK_PYTHON="python3"
  fi
  
  if [ ! "$USER_SPARK_OPTS" = "" ]; then
    SPARK_OPTS=$USER_SPARK_OPTS
    SPARK_PACKAGES=$(ruby -e "opts='$SPARK_OPTS'.split;pkgs=nil;opts.each_with_index{|o,i| pkgs=opts[i+1] if o.start_with?('--packages')};puts pkgs || '$SPARK_PACKAGES'")
    export SPARK_OPTS
    export SPARK_PACKAGES
    
    sudo jupyter toree install --interpreters=$INTERPRETERS --spark_home=$SPARK_HOME --python_exec=$PYSPARK_PYTHON --spark_opts="$SPARK_OPTS"
    # NOTE - toree does not pick SPARK_OPTS, so use the following workaround until it's fixed  
    if [ ! "$SPARK_PACKAGES" = "" ]; then
      if ! grep "spark.jars.packages" /etc/spark/conf/spark-defaults.conf; then
        sudo bash -c "echo 'spark.jars.packages              $SPARK_PACKAGES' >> /etc/spark/conf/spark-defaults.conf"
      fi
    fi
  else
    sudo jupyter toree install --interpreters=$INTERPRETERS --spark_home=$SPARK_HOME --python_exec=$PYSPARK_PYTHON
  fi

  
  if [ "$INSTALL_PY3_PKGS" = true ]; then
    sudo bash -c 'echo "" >> /etc/spark/conf/spark-env.sh'
    sudo bash -c 'echo "export PYSPARK_PYTHON=/usr/bin/python3" >> /etc/spark/conf/spark-env.sh'
    
    #if [ -f /usr/local/share/jupyter/kernels/apache_toree_pyspark/kernel.json ]; then
    #  sudo bash -c 'sed -i "s/\"PYTHON_EXEC\": \"python\"/\"PYTHON_EXEC\": \"\/usr\/bin\/python3\"/g" /usr/local/share/jupyter/kernels/apache_toree_pyspark/kernel.json'
    #fi
    
  fi
  
  # the following dirs could cause conflict, so remove them
  rm -rf ~/.m2/
  rm -rf ~/.ivy2/
  
  if [ "$NO_JUPYTER" = false ]; then
    echo "Starting Jupyter notebook via pyspark"
    cd ~
    #PYSPARK_DRIVER_PYTHON=jupyter PYSPARK_DRIVER_PYTHON_OPTS="notebook --no-browser" pyspark > /var/log/jupyter/jupyter.log &
    if [ "$BIGDL" = false ]; then
      sudo puppet apply << PUPPET_SCRIPT
      include 'upstart'
      upstart::job { 'jupyter':
        description    => 'Jupyter',
        respawn        => true,
        respawn_limit  => '0 10',
        start_on       => 'runlevel [2345]',
        stop_on        => 'runlevel [016]',
        console        => 'output',
        chdir          => '/home/hadoop',
        script           => '
        sudo su - hadoop > /var/log/jupyter/jupyter.log 2>&1 <<BASH_SCRIPT
        export NODE_PATH="$NODE_PATH"
        export PYSPARK_DRIVER_PYTHON="jupyter"
        export PYSPARK_DRIVER_PYTHON_OPTS="notebook --no-browser $SSL_OPTS_JUPYTER --log-level=INFO"
        export NOTEBOOK_DIR="$NOTEBOOK_DIR"
        pyspark
BASH_SCRIPT
        ',
      }
PUPPET_SCRIPT
    else
      setup_jupyter_process_with_bigdl
    fi
  fi
}

create_hdfs_user() {
  wait_for_spark
  sudo -u hdfs hdfs dfs -mkdir /user/$JUPYTER_HUB_DEFAULT_USER
  sudo -u hdfs hdfs dfs -chown $JUPYTER_HUB_DEFAULT_USER:$JUPYTER_HUB_DEFAULT_USER /user/$JUPYTER_HUB_DEFAULT_USER
  sudo -u hdfs hdfs dfs -chmod -R 777 /user/$JUPYTER_HUB_DEFAULT_USER
}

# apache toree install
if [ "$TOREE_KERNEL" = true ]; then
  echo "Running background process to install Apacke Toree"
  # spark 1.6
  #sudo pip install --pre toree
  #sudo jupyter toree install

  # spark 2.0
  cd /mnt
  if [ "$USE_CACHED_DEPS" != true ]; then
    curl https://bintray.com/sbt/rpm/rpm | sudo tee /etc/yum.repos.d/bintray-sbt-rpm.repo
    sudo yum install docker sbt -y
  fi
  if [ ! "$USE_CACHED_DEPS" = true ]; then
    git clone https://github.com/apache/incubator-toree.git
  fi
  cd incubator-toree/
  git pull
  export APACHE_SPARK_VERSION=$APACHE_SPARK_VERSION
  make -j $NPROC dist 
  make clean release APACHE_SPARK_VERSION=$APACHE_SPARK_VERSION || true # gettting the docker not running error, swallow it with || true
  if [ "$RUN_AS_STEP" = true ]; then
    background_install_proc
  else
    background_install_proc &
  fi
else
  if [ "$NO_JUPYTER" = false ]; then
    echo "Starting Jupyter notebook"
    if [ "$BIGDL" = false ]; then
      sudo puppet apply << PUPPET_SCRIPT
      include 'upstart'
      upstart::job { 'jupyter':
          description    => 'Jupyter',
          respawn        => true,
          respawn_limit  => '0 10',
          start_on       => 'runlevel [2345]',
          stop_on        => 'runlevel [016]',
          console        => 'output',
          chdir          => '/home/hadoop',
          env            => { 'NOTEBOOK_DIR' => '$NOTEBOOK_DIR', 'NODE_PATH' => '$NODE_PATH' },
          exec           => 'sudo su - hadoop -c "jupyter notebook --no-browser $SSL_OPTS_JUPYTER" > /var/log/jupyter/jupyter.log 2>&1',
      }
PUPPET_SCRIPT
    else
      setup_jupyter_process_with_bigdl &
    fi
  fi
fi

if [ "$JUPYTER_HUB" = true ]; then
  sudo npm install -g --unsafe-perm configurable-http-proxy
  sudo python3 -m pip install jupyterhub #notebook ipykernel
  #sudo python3 -m ipykernel install
  
  if [ ! "$JUPYTER_HUB_DEFAULT_USER" = "" ]; then
    create_hdfs_user &
  fi
  # change the password of the hadoop user to JUPYTER_PASSWORD
  if [ ! "$JUPYTER_PASSWORD" = "" ]; then
    sudo sh -c "echo '$JUPYTER_PASSWORD' | passwd $JUPYTER_HUB_DEFAULT_USER --stdin"
  fi
  
  sudo ln -sf /usr/local/bin/jupyterhub /usr/bin/
  sudo ln -sf /usr/local/bin/jupyterhub-singleuser /usr/bin/
  mkdir -p /mnt/jupyterhub
  cd /mnt/jupyterhub
  echo "Starting Jupyterhub"
  #sudo jupyterhub $SSL_OPTS_JUPYTERHUB --port=$JUPYTER_HUB_PORT --ip=$JUPYTER_HUB_IP --log-file=/var/log/jupyter/jupyterhub.log --config ~/.jupyter/jupyter_notebook_config.py &
  sudo puppet apply << PUPPET_SCRIPT
  include 'upstart'
  upstart::job { 'jupyterhub':
      description    => 'JupyterHub',
      respawn        => true,
      respawn_limit  => '0 10',
      start_on       => 'runlevel [2345]',
      stop_on        => 'runlevel [016]',
      console        => 'output',
      chdir          => '/mnt/jupyterhub',
      env            => { 'NOTEBOOK_DIR' => '$NOTEBOOK_DIR', 'NODE_PATH' => '$NODE_PATH' },
      exec           => 'sudo /usr/bin/jupyterhub --pid-file=/var/run/jupyter.pid $SSL_OPTS_JUPYTERHUB --port=$JUPYTER_HUB_PORT --ip=$JUPYTER_HUB_IP --log-file=/var/log/jupyter/jupyterhub.log --config /home/hadoop/.jupyter/jupyter_notebook_config.py'
  }
PUPPET_SCRIPT

fi

cat << 'EOF' > /tmp/jupyter_logpusher.config
{
  "/var/log/jupyter/" : {
    "includes" : [ "(.*)" ],
    "s3Path" : "node/$instance-id/applications/jupyter/$0",
    "retentionPeriod" : "5d",
    "logType" : [ "USER_LOG", "SYSTEM_LOG" ]
  }
}
EOF
cat /tmp/jupyter_logpusher.config | sudo tee -a /etc/logpusher/jupyter.config

fi
echo "Bootstrap action finished"
