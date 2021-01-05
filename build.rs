fn main() {
    tonic_build::configure()
        .build_server(false)
        .compile(&["proto/remote/kv.proto"], &["proto/remote"])
        .unwrap();
}
