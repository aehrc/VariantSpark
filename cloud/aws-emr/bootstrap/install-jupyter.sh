#!/bin/bash
set -x -e

INPUT_PATH=""
HAIL_VERSION="0.1"
SPARK_VERSION="2.2.1"
IS_MASTER=false

if grep isMaster /mnt/var/lib/info/instance.json | grep true;
then
  IS_MASTER=true
fi

while [ $# -gt 0 ]; do
    case "$1" in
    --input-path)
      shift
      INPUT_PATH=$1
      ;;
    --hail-version)
      shift
      HAIL_VERSION=$1
      ;;
    --spark-version)
      shift
      SPARK_VERSION=$1
      ;;
    --notebookPath)
      shift
      NotebookPath=$1
      ;;
    --path-prefix)
      shift
      PATH_PREFIX=$1
      ;;
    -*)
      error_msg "unrecognized option: $1"
      ;;
    *)
      break;
      ;;
    esac
    shift
done

BUCKET=$(awk -v XX="$NotebookPath" 'BEGIN{x=substr(XX,6); split(x,a,"/"); print(a[1])}')
PREFIX=$(awk -v XX="$NotebookPath" -v YY="$BUCKET" 'BEGIN{y=length(YY); print(substr(XX,7+y));}')

wget "${PATH_PREFIX}/cloud/aws-emr/cf-templates/VariantSpark_example_with_Hail_library.ipynb"
aws s3 cp VariantSpark_example_with_Hail_library.ipynb $NotebookPath/

upstart_jupyter() {
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
    sudo su - hadoop > /home/hadoop/jupyter.log 2>&1 <<BASH_SCRIPT
export SPARK_HOME=/usr/lib/spark
export PYTHONPATH=$PYTHONPATH:/home/hadoop/hail-python.zip
/home/hadoop/miniconda2/envs/jupyter/bin/jupyter notebook
BASH_SCRIPT
    ',
  }
PUPPET_SCRIPT
}


if [ "$IS_MASTER" = true ]; then
    #Install miniconda
    wget https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh 
    sh Miniconda2-latest-Linux-x86_64.sh -b
    export PATH=~/miniconda2/bin:$PATH
    conda create -y -n jupyter python=2.7
    source activate jupyter
    #Install other packages
    #TODO: make these configurable
    pip install --upgrade matplotlib pandas click variant-spark
    ln -s "/home/hadoop/miniconda2/envs/jupyter/lib/python2.7/site-packages/varspark/jars/variant-spark"*.jar "/home/hadoop/miniconda2/envs/jupyter/lib/python2.7/site-packages/varspark/jars/varspark.jar"
    #Install jupyter components
    pip install --upgrade jupyter==1.0.0 s3contents==0.1.4 decorator==4.2.1 notebook==5.7.0 juspark 
    mkdir -p ~/.jupyter
    cat >> ~/.jupyter/jupyter_notebook_config.py << EOF
# S3ContentsManager
from s3contents import S3ContentsManager
c.NotebookApp.contents_manager_class = S3ContentsManager
c.S3ContentsManager.bucket_name = "$BUCKET"
c.S3ContentsManager.prefix = "$PREFIX" 
EOF

    cat >> ~/.jupyter/jupyter_notebook_config.py << EOF
c.NotebookApp.token = ''
c.NotebookApp.password = ''
c.NotebookApp.ip = '*'   
c.NotebookApp.open_browser = False
c.NotebookApp.allow_remote_access = True
EOF

    #  Setup JuSpark kernel

    mkdir -p  ~/.local/share/jupyter/kernels/juspark
    cat > ~/.local/share/jupyter/kernels/juspark/kernel.json << EOF
{
 "display_name": "JuSpark",
 "language": "python",
 "argv": [
  "/home/hadoop/miniconda2/envs/jupyter/bin/python",
  "-m",
  "ipykernel",
  "-f",
  "{connection_file}",
  "--ext=juspark"
 ]
}
EOF

    # Setup profiles for juspark
    mkdir -p ~/.juspark/profiles
    cat > ~/.juspark/profiles/hail << EOF
{
    "spark.jars":"/home/hadoop/hail-all-spark.jar",
    "spark.submit.pyFiles":"/home/hadoop/hail-python.zip"
}
EOF

    #Install puppet modules
    sudo puppet module install spantree-upstart
    
    #Setup daemons
    upstart_jupyter

fi
