# Contributing to CurateND

## Getting Started
Bendo is a go package. There are several steps you must complete before you can to compile, test, and commit to the project.

### Development Dependencies
Bendo requires `go` and `godep` to compile.

```console
brew update
brew install go godep
```

> These homebrew recipes are already included [DLT dotfiles](https://github.com/ndlib/dlt-dotfiles).

### Preparing the Environment
Go has built-in dependency management that performs functions inside the directory that is set as `$GOPATH`. Our convention for development environments is to set `$GOPATH` to `~/gocode`.

```console
mkdir ~/gocode
export GOPATH='~/gocode'
```

> If you manage your shell environment with [DLT dotfiles](https://github.com/ndlib/dlt-dotfiles) it will set this up for you.

To verify that `$GOPATH` is configured correctly try:

```console
echo $GOPATH
```

It should return `/Users/<YOUR_USERNAME>/gocode`.

### Checking out the Codebase
Once `$GOPATH` is configured use `go get` to check out the git repository and keep track of it so it can be included by other go projects.

```console
go get -d github.com/ndlib/bendo
```

### Configuring git
The git repository configuration used by `go get` is not set up to make commits back to the project. If you will be changes to Bendo you will need to reconfigure it.

```console
cd $GOPATH/src/github.com/ndlib/bendo
git remote set-url origin git@github.com:ndlib/bendo.git
```

> This remote URL assumes you have commit access to ndlib/bendo. If you are working on another repo, like a fork, use the URL provided buy github for that repo.

### Building Bendo
Bendo does not track its executables in source control, they must be built from source.

```console
cd $GOPATH/src/github.com/ndlib/bendo
make
```

After running `make` you should have four executables:

```console
ls -1 $GOPATH/src/github.com/ndlib/bendo/bin
bclient
bendo
bstress
butil
```

## Running Bendo
There are also several setup steps in order to _run_ Bendo.

### Directory Setup
By convention we will run Bendo out of the `~/goapps` directory. Bendo expects the presence of to directories: `bendo_cach` and `bendo_storage`.

```console
mkdir -p ~/goapps/bendo/{bendo_cache,bendo_storage}
```

### Configuration File
Many aspects of Bendo are configurable via a config file. The sample configuration file contains a MySQL directive that is not necessary for running the application in development.

```console
sed '/Mysql/d' $GOPATH/src/github.com/ndlib/bendo/sample.config > ~/goapps/bendo/development.config
```

### Starting the Application
```console
cd ~/goapps/bendo
bendo -config-file development.config
```

> The `bendo` command is added to your `$PATH` by [DLT-dotfiles](https://github.com/ndlib/dlt-dotfiles). If you need to call it directly use `$GOPATH/src/github.com/ndlib/bendo/bin/bendo`.
