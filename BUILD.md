# Instructions for building Akula

On debian-based systems you may need to install the following packages:

| Package           |  Purpose                                      |
|----------------   |---------------------------------------------  |
| e2fslibs-dev      |  Used by libmdbx to set file flags.           |
| clang             |  Used to build C++ dependencies.              |
| cmake             |  Used to build C++ dependencies.              |

To build the akula binary:

```
cargo build
```

To get a list of options:

```
cargo run -- --help
```

To run the binary:

```
cargo run -- --help
```
