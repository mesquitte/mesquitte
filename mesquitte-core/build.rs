fn main() {
    tonic_build::configure()
        .compile_well_known_types(true)
        .compile_protos(&["src/idl/sample.proto"], &["idl"])
        .expect("failed to build protobuf.");
}
