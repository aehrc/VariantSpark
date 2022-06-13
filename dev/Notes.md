

## Gihub actions


This is some helpful info: https://blog.eizinger.io/12274/using-github-actions-and-gitflow-to-automate-your-release-process
And here is the doco for github actions: https://docs.github.com/en/actions/guides/building-and-testing-java-with-maven

## Snapshot Release of python package

in `python`

    python setup.py sdist
    
to deploy:

    export PYPI_URL=https://upload.pypi.org/legacy/
    export PYPI_USER=piotrszul
    export PYPI_PASSWORD='M..new0'
    twine upload --repository-url ${PYPI_URL} --username ${PYPI_USER} --password ${PYPI_PASSWORD} dist/* 

## Release to Maven Central

Pre-requisites:

- pgp installed 
- a default pgp key generated in registered in a public registry ( e.g. keys.openpgp.org)
- `~/.m2/settting.xml` with credentials to oss.sonatype.org


    export GPG_TTY=$(tty)
    mvn -DskipTests -P release deploy
    mvn nexus-staging:release
    
Note: Snapshot releases do not require the last step (i.e. after maven deploy the should be already fully deployed)
    
More info:  https://central.sonatype.org/pages/apache-maven.html#performing-a-release-deployment-with-the-maven-release-plugin

see: `~/.m2/settting.xml for all the passwords etc`

Also see: 

- https://docs.github.com/en/authentication/managing-commit-signature-verification/generating-a-new-gpg-key  for how to generate a new PGP key.
- https://keys.openpgp.org/about/usage  on how to register a pgp key in a public repository.
- https://oss.sonatype.org/#welcome to visit the actual hosting repositories.


## Setting up an AWS vm for Hail compilation


Setup:

    sudo yum groupinstall 'Development Tools'
    sudo yum install java-1.8.0-openjdk-devel
    sudo yum install cmake
    sudo yum install gcc
    sudo yum install git
   
 
 Compilation and copy:
 
    ./gradlew -Dspark.version=2.3.0 clean shadowJar
    aws s3 cp build/libs/hail-all-spark.jar s3://variant-spark/deps/hail-0.1/hail-0.1-74bf1ebb3edc_2.3.0-all.jar