# Instructions for building Akula

On debian-based systems you may need to install the following packages:

| Package           |  Purpose                                      |
|----------------   |---------------------------------------------  |
| e2fslibs-dev      |  Used by libmdbx to set file flags.           |


We also need protobuf-compiler, but the current version is too early
and you will need to build from source:

```
git clone --depth 1 https://github.com/protocolbuffers/protobuf.git
cd protobuf/
git submodule update --init --recursive
cmake . -B build/
cd build
make
cd ..
cp protoc ~/.cargo/bin
which protoc
protoc --version
```

The version should be >= 3.15
