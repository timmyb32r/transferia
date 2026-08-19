use base64::Engine as _;
use prost::Message as _;
use prost_reflect::DescriptorPool;

pub fn protobuf_descriptor_pool(definition: &str) -> anyhow::Result<(DescriptorPool, String)> {
    let descriptor = base64::engine::general_purpose::STANDARD
        .decode(definition)
        .ok()
        .and_then(|bytes| {
            prost_reflect::prost_types::FileDescriptorProto::decode(bytes.as_slice()).ok()
        })
        .map_or_else(
            || {
                protox::file::File::from_source("schema-registry.proto", definition)
                    .map(|file| file.file_descriptor_proto().clone())
                    .map_err(anyhow::Error::from)
            },
            Ok,
        )?;
    let file_name = descriptor
        .name
        .clone()
        .ok_or_else(|| anyhow::anyhow!("Protobuf file descriptor has no name"))?;
    let mut pool = DescriptorPool::new();
    pool.add_file_descriptor_proto(descriptor)?;
    Ok((pool, file_name))
}
