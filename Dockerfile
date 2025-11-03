# Use Ubuntu 20.04 as the base image
FROM --platform=linux/amd64 ubuntu:20.04

# Set working directory
WORKDIR /app

# Avoid interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install Python 3.7, pip, Java 8, and Git
RUN apt-get update && apt-get install -y \
    python3-pip \
    openjdk-8-jdk \
    git wget \
    && rm -rf /var/lib/apt/lists/*

# Set Python 3.8 as default python
RUN ln -s /usr/bin/python3.8 /usr/bin/python

# Set JAVA_HOME environment variable, 
# openjdk-8-jdk for linux/amd64, it installs java-8-openjdk-amd64.
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Apache Spark 3.1.1 with Hadoop 3.2, only work for linux/amd64
RUN wget -q https://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz && \
    tar -xzf spark-3.1.1-bin-hadoop3.2.tgz -C /opt && \
    rm spark-3.1.1-bin-hadoop3.2.tgz && \
    ln -s /opt/spark-3.1.1-bin-hadoop3.2 /opt/spark

# Set Spark environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
# ENV PYSPARK_PYTHON=python3.8
# ENV PYSPARK_DRIVER_PYTHON=python3.8

# Install VariantSpark
RUN pip3 install --no-cache-dir variant-spark

# Install matching PySpark + Hail
RUN pip install --no-cache-dir pyspark==3.1.1 variant-spark hail==0.2.74 \
Jinja2==3.0.3 \
pandas==1.1.4 \
typedecorator==0.0.5 \
scipy==1.6.3 \
numpy==1.21.2 \
patsy==0.5.2 \
statsmodels==0.13.2 \
seaborn==0.11.2 \
chardet==3.0.4 \
google-auth==1.35.0 \
google-cloud-core==1.7.3 \
google-cloud-storage==1.25.0 \

# Clone the VariantSpark repository
# RUN git clone https://github.com/aehrc/VariantSpark.git

# Set working directory to the cloned repository
WORKDIR /app/VariantSpark
RUN ln -s /usr/local/share/variant-spark/data/ .

# Command to run
CMD ["variant-spark -h"]
