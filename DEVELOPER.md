### Testing

Unit tests and Integration tests are an essential part of code contributions.

To mark a Java test as a Java integration test, add the annotation `@Category(IntegrationTest.class)` to the test class definition as well as to its hierarchy of superclasses. Java integration tests can be in the same package as Java unit tests.
 
```java
    @Category(IntegrationTest.class)
    public class MyIntegrationTest {
    ...
    }
```
 
To mark a Clojure test as Clojure integration test, the test source must be located in a package with name prefixed by `integration.`

For example, the test `test/clj/org.apache.storm.drpc_test.clj` is considered a clojure unit test, whereas
 `test/clj/integration.org.apache.storm.drpc_test.clj` is considered a clojure integration test.

Please refer to section <a href="#building">Build the code and run the tests</a> for how to run integration tests, and the info on the build phase each test runs. 

<a name="contribute-documentation"></a>

# Build the code and run the tests

## Prerequisites
First of all you need to make sure you are using maven 3.2.5 or below.  There is a bug in later versions of maven as linked to from https://issues.apache.org/jira/browse/MSHADE-206 that
cause shaded dependencies to not be packaged correctly.  Also please be aware that because we are shading dependencies mvn dependency:tree will not always show the dependencies correctly. 

In order to build `storm` you need `python`, `ruby` and `nodejs`. In order to avoid an overfull page we don't provide platform/OS specific installation instructions for those here. Please refer to you platform's/OS' documentation for support.

The `ruby` package manager `rvm` and `nodejs` package manager `nvm` are for convenience and are used in the tests which run on [travis](https://travis-ci.org/apache/storm). They can be installed using `curl -L https://get.rvm.io | bash -s stable --autolibs=enabled && source ~/.profile` (see the [rvm installation instructions](https://github.com/rvm/rvm) for details) and `wget -qO- https://raw.githubusercontent.com/creationix/nvm/v0.26.1/install.sh | bash && source ~/.bashrc` (see the [nvm installation instructions](https://github.com/creationix/nvm) for details).

With `rvm` and `nvm` installed you can run

```sh
rvm use 2.4.2 --install
nvm install 8.9.3
nvm use 8.9.3
```

in order to get started as fast as possible. Users can still install a specific version of `ruby` and/or `node` manually.

## Building

The following commands must be run from the top-level directory.

`mvn clean install`

If you wish to skip the unit tests you can do this by adding `-DskipTests` to the command line. 

In case you modified `storm.thrift`, you have to regenerate thrift code as java and python code before compiling whole project.

```sh
cd storm-client/src
sh genthrift.sh
```

## Testing

Tests are separated in two groups, Unit tests, and Integration tests. Java unit tests, Clojure unit tests, and Clojure integration tests (for reasons inherent to the clojure-maven-plugin) run in the maven `test` phase. Java integration tests run in the maven `integration-test` or `verify` phases. 
 
To run Clojure and Java unit tests but no integration tests execute the command
 
    mvn test

Integration tests require that you activate the profile `integration-test` and that you specify the `maven-failsafe-plugin` in the module pom file.
 
To run all Java and Clojure integration tests but no unit tests execute one of the commands
 
    mvn -P  integration-tests-only verify
    mvn -P  integration-tests-only integration-test

To run all unit tests plus Clojure integration tests but no Java integration tests execute the command
 
    mvn -P all-tests test

To run all unit tests and all integration tests execute one of the commands
 
    mvn -P all-tests verify
    mvn -P all-tests integration-test
 
 
You can also run tests selectively via the Clojure REPL.  The following example runs the tests in
[auth_test.clj](storm-core/test/clj/org/apache/storm/security/auth/auth_test.clj), which has the namespace
`org.apache.storm.security.auth.auth-test`.

You can also run tests selectively with `-Dtest=<test_name>`.  This works for both clojure and junit tests.

> Tip: IDEs such as IntelliJ IDEA support a built-in Clojure REPL, which you can also use to run tests selectively.
> Sometimes you may find that tests pass/fail depending on which REPL you use, which -- although frustrating --
> can be helpful to narrow down errors.

Unfortunately you might experience failures in clojure tests which are wrapped in the `maven-clojure-plugin` and thus doesn't provide too much useful output at first sight - you might end up with a maven test failure with an error message as unhelpful as `Clojure failed.`. In this case it's recommended to look into `target/test-reports` of the failed project to see what actual tests have failed or scroll through the maven output looking for obvious issues like missing binaries.

By default integration tests are not run in the test phase. To run Java and Clojure integration tests you must enable the profile
 


<a name="packaging"></a>

## Create a Storm distribution (packaging)

You can create a _distribution_ (like what you can download from Apache) as follows.  Note that the instructions below
do not use the Maven release plugin because creating an official release is the task of our release manager.

    # First, build the code.
    $ mvn clean install # you may skip tests with `-DskipTests=true` to save time

    # Create the binary distribution.
    $ cd storm-dist/binary && mvn package

The last command will create Storm binaries at:

    storm-dist/binary/target/apache-storm-<version>.pom
    storm-dist/binary/target/apache-storm-<version>.tar.gz
    storm-dist/binary/target/apache-storm-<version>.zip

including corresponding `*.asc` digital signature files.

After running `mvn package` you may be asked to enter your GPG/PGP credentials (once for each binary file, in fact).
This happens because the packaging step will create `*.asc` digital signatures for all the binaries, and in the workflow
above _your_ GPG private key will be used to create those signatures.

You can verify whether the digital signatures match their corresponding files:

    # Example: Verify the signature of the `.tar.gz` binary.
    $ gpg --verify storm-dist/binary/target/apache-storm-<version>.tar.gz.asc


<a name="best-practices"></a>

# Best practices


<a name="best-practices-testing"></a>

## Testing

Tests should never rely on timing in order to pass.  Storm can properly test functionality that depends on time by
simulating time, which means we do not have to worry about e.g. random delays failing our tests indeterministically.

If you are testing topologies that do not do full tuple acking, then you should be testing using the "tracked
topologies" utilities in `org.apache.storm.testing.clj`.  For example,
[test-acking](storm-core/test/clj/org/apache/storm/integration_test.clj) (around line 213) tests the acking system in
Storm using tracked topologies.  Here, the key is the `tracked-wait` function: it will only return when both that many
tuples have been emitted by the spouts _and_ the topology is idle (i.e. no tuples have been emitted nor will be emitted
without further input).  Note that you should not use tracked topologies for topologies that have tick tuples.

<a name="best-practices-versions"></a>

## Version Changes

An easy way to change versions across all pom files, for example from `1.0.0-SNAPSHOT` to `1.0.0`, is with the maven
versions plugin.

```
mvn versions:set #This prompts for a new version
mvn versions:commit
```

[Plugin Documentation] (http://www.mojohaus.org/versions-maven-plugin/)

<a name="tools"></a>



