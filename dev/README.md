Development notes
=================


# Release process (WIP)

For release with version: <version>

- create a release branch  `release/<version>` of `master`
- bump the version in pom.xml to `<version>`
- commit and push then new branch
- create a release pull request from `release/<version>` against `master`
- test and fix issue
- when ready merge the release branch to master
- tag the merge commit with `v<version>`
- create and publish github release based on the tag 
- verify that the deployment action has run and that the release got deployed 
- bump the version in pom.xml to `next(<version>)-SNAPSHOT`
