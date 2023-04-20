fn main() {
    let proto = "../proto/communication.proto";
    let include = "../proto";

    tonic_build::configure()
        .build_client(false)
        .build_server(true)
        .out_dir("./src")
        .compile(&[proto], &[include])
        .unwrap_or_else(|e| panic!("protobuf compile error: {}", e));

    println!("cargo:rerun-if-changed={}", proto);
}
