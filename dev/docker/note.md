
try https://variantspark.readthedocs.io/en/latest/getting_started.html
## ask gpt to create dockerfile
	- any python base image eg. FROM python:3.8, don't support openjdk package installation. 
	- try ubuntu which set python 3.8 as default. 

## test it interactively locl
- docker build -t vsapp .
- docker run -it --name vsrun1 vsapp 
```
	python --version  # Should show Python 3.8.x
	java -version  # Should show OpenJDK 8
	pip3 show variant-spark # To find where variant-spark is installed 	
```
- docker cp variantspark_script.py vsrun2:/app/VariantSpark/variantspark_script.py # copy file from local to docker
- vs works without mvn install but only pip install inside docker container
	- variant-spark importance -if gitHub/VariantSpark/data/chr22_1000.vcf -ff gitHub/VariantSpark/data/chr22-labels.csv -fc 22_16050408 -v -rn 500 -rbs 20 -ro -sr 13
	```
		root@16542009db87:/app/VariantSpark# variant-spark importance -if gitHub/VariantSpark/data/chr22_1000.vcf -ff gitHub/VariantSpark/data/chr22-labels.csv -fc 22_16050408 -v -rn 500 -rbs 20 -ro -sr 13
		25/10/27 08:41:06 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
		log4j:WARN No appenders could be found for logger (au.csiro.variantspark.cli.ImportanceCmd).
		...
		Last build trees: 20, time: 779 ms, timePerTree: 38 ms
		Finished trees: 500, current oobError: 0.016483516483516484, totalTime: 36.185 s,  avg timePerTree: 0.07237 s
		Last build trees: 20, time: 675 ms, timePerTree: 33 ms
		Random forest oob accuracy: 0.016483516483516484, took: 36.4 s
		variable,importance
		22_16050408_T_C,18.484457676767143
		22_16051480_T_C,17.593204808682323
		...
	```
	- variant-spark --spark --master 'local[*]' -- importance -if gitHub/VariantSpark/data/chr22_1000.vcf -ff gitHub/VariantSpark/data/chr22-labels.csv -fc 22_16050408 -v -rn 500 -rbs 20 -ro -sr 13
- pip3 install --no-cache-dir -r /app/VariantSpark/requirements.txt # install python dependency

## about pip install variant-spark
- variant-spark_2.12-0.5.5-all.jar : installed by pip install variant-spark
	- jar tf variant-spark_2.12-0.5.5-all.jar
		- jar is included inside the python package: 
		- /usr/local/lib/python3.8/dist-packages/varspark/jars/variant-spark_2.12-0.5.5-all.jar
		- but this jar is not fat jar which didn't includes au.csiro.aehrc.third.hail-is
- hail-all-spark.jar : installed by pip3 install hail==0.2.74 inside the requirement.txt
 - is used by the Python hail package at runtime.
 - /usr/local/lib/python3.8/dist-packages/hail/backend/hail-all-spark.jar
 - jar tf hail-all-spark.jar | grep hail | grep SparkBackend  

- mvn install with hail
	- Maven will try to download a JAR matching hail_2.12_3.1:0.2.74 from repo: au.csiro.aehrc.third.hail-is based on pom.xml
	- the JAR is stored in your local Maven repository (~/.m2/repository/au/csiro/aehrc/third/hail-is/hail_2.12_3.1/0.2.74/).
	 
- refer to src/main/scala/au/csiro/variantspark/hail/methods/RFModel.scala
	- ~/.m2/repository/au/.../hail_2.12_3.1/0.2.74/ is called during mvn test or pure scala code running
	- python: vshl.init() adds hail-all-spark.jar to the Spark classpath.
	- python: spark = SparkSession.builder.config('spark.jars', vs.find_jar()).getOrCreate() adds spark.jars to spark classpath
	- python: vshl.random_forest_model(...) calls scala RFModel.scala based on park classpath
	- summary: python calls scala depend on hail-all-spark.jar but not mvn installed hails

- which variant-spark # to find variant-spark bash script
   - orginal from https://github.com/aehrc/VariantSpark/tree/master/bin/variant-spark
   - it requires to set up spark




## Docker Build on ARM vs. AMD64
- `docker build -t vsapp . ` on your Mac (with an ARM-based chip like M1/M2), Docker builds the image for the native architecture, which is linux/arm64.
- `docker build --platform linux/amd64 -t vsapp .` you instruct Docker to build the image for the linux/amd64 architecture, even on your ARM-based Mac.
- The openjdk-8-jdk package in Ubuntu’s repositories is architecture-specific. For linux/arm64, it installs java-8-openjdk-arm64; for linux/amd64, it installs java-8-openjdk-amd64.

- `uname -m` # shows x86_64 for AMD64; or aarch64 for ARM64


# optimize dockerfile with two layout dockerfile

# to do list
- pip3 show variant-spark shows Version: 0.5.5 but author Piotr Szul et. al is wrong
- pip3 install variant-spark, not automatically install pyspark as a dependency, got error
  ```
  	from pyspark import SparkConf
	ModuleNotFoundError: No module named 'pyspark'
  ```
  - pip3 show Jinja2 pandas typedecorator hail pyspark scipy numpy patsy statsmodels seaborn # only typedecorator installed
	```
	root@16542009db87:/app/VariantSpark# pip3 show Jinja2 pandas typedecorator hail pyspark scipy numpy patsy statsmodels seaborn
	WARNING: Package(s) not found: Jinja2, hail, numpy, pandas, patsy, pyspark, scipy, seaborn, statsmodels
	Name: typedecorator
	Version: 0.0.5
	Summary: Decorator-based type checking library for Python 2 and 3
	Home-page: https://github.com/dobarkod/typedecorator/
	Author: Senko Rasic
	Author-email: senko.rasic@goodcode.io
	License: MIT
	Location: /usr/local/lib/python3.8/dist-packages
	Requires: 
	Required-by: variant-spark

	```
