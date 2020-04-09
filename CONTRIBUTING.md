Contributing to VariantSpark
=============================

# Contributing Code Changes
Please review the preceding section before proposing a code change. This section documents how to do so.

When you contribute code, you affirm that the contribution is your original work and that you license the work to the project under the project’s open source license. Whether or not you state this explicitly, by submitting any copyrighted material via pull request, email, or other means you agree to license the material under the project’s open source license and warrant that you have the legal authority to do so.

## Issue management

Generally, _VariantSpark_ uses _Github Issues_ to track logical issues, including bugs and improvements, and uses _Github_ pull requests to manage the review and merge of specific code changes. That is, _Github Issues_ are used to describe what should be fixed or changed, and high-level approaches, and pull requests describe how to implement that change in the project’s source code. For example, major design decisions are discussed in _Github Issues_

## Pull Request

1. Fork the Github repository at https://github.com/aehrc/VariantSpark if you haven’t already
2. Clone your fork, create a new branch, push commits to the branch.
3. Consider whether documentation or tests need to be added or updated as part of the change, and add them as needed.
4. Run all tests with `./dev/build.sh` to verify that the code still compiles, passes tests, and passes style checks. If style checks fail, review the Code Style Guide below.
5. Open a pull request against the master branch of aehrc/VariantSpark. (Only in special cases would the PR be opened against other branches.)
    * The PR title should be of the form `[#XXXX] Title`, where `#XXXX` is the relevant _Github Issue_ number, and Title may be the issue title or a more specific title describing the PR itself.
    * If the pull request is still a work in progress, and so is not ready to be merged, but needs to be pushed to Github to facilitate review, then add [WIP] after the issue number.
    
6. Your our pull request will automatically be linked to an issue. There is no need to be the Assignee of the issue to work on it, though you are welcome to comment that you have begun work.
7. The Travis-CI automatic pull request builder will test your changes
8. After about 10 minues, Travic-CI will post the results of the test to the pull request, along with a link to the full results on Jenkins.
9. Watch for the results, and investigate and fix failures promptly
   * Fixes can simply be pushed to the same branch from which you opened your pull request
   * Travis-CI will automatically re-test when new commits are pushed

## Closing Your Pull Request / Issue

* If a change is accepted, it will be merged and the pull request will automatically be closed, along with the associated issue if any

## Code Style Guide

Follow the [Apache Spark - Code Style Guide](http://spark.apache.org/contributing.html#code-style-guide)

In particular:

* For Python code follow PEP 8 with one exception: lines can be up to 100 characters in length, not 79.
* Scala code is formatted with `scalafmt` with the configuration file in .scalafmt.conf - maven build 
checks the formatting but does not apply automatically. You can setup you IDE (e.g. IntelJ) to apply the 
formatting or run `./dev/scalafmt` to apply formatting to all new files.

