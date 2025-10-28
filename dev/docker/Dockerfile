# Use Ubuntu 20.04 as the base image
FROM --platform=linux/amd64 ubuntu:20.04

# Set working directory
WORKDIR /app

# Avoid interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install Python 3.8, pip, Java 8, and Git
RUN apt-get update && apt-get install -y \
    python3.8 \
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

# Install Apache Spark 3.1.2 with Hadoop 3.2, only work for linux/amd64
RUN wget -q https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz && \
    tar -xzf spark-3.1.2-bin-hadoop3.2.tgz -C /opt && \
    rm spark-3.1.2-bin-hadoop3.2.tgz && \
    ln -s /opt/spark-3.1.2-bin-hadoop3.2 /opt/spark

# Set Spark environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
# ENV PYSPARK_PYTHON=python3.8
# ENV PYSPARK_DRIVER_PYTHON=python3.8

# Install VariantSpark
RUN pip3 install --no-cache-dir variant-spark

# Clone the VariantSpark repository
# RUN git clone https://github.com/aehrc/VariantSpark.git

# Set working directory to the cloned repository
WORKDIR /app/VariantSpark

# Command to run
CMD ["bash"]
