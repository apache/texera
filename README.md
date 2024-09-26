<h1 align="center">IcedTea - Efficient and Responsive Time-Travel Debugging in Dataflow Systems.</h1>

This repo is an implementation of IcedTea built on top of an open-source data processing platform, [Texera](https://github.com/texera/texera).

## Prerequisites:
1. Java 11
2. Node JS LTS version
3. SBT
4. Yarn

You can follow [this guide](https://github.com/Texera/texera/wiki/Getting-Started) to install all the requirements.

## To run the project:
First, install frontend packages by running this command in bash:
```agsl
core/scripts/build.sh
```

Then, start both frontend and backend by running this command:
```agsl
core/scripts/deploy-daemon.sh
```

## Time-travel configuration:
