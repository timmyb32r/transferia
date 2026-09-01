use std::collections::HashMap;
use std::path::Path;

use base64::Engine as _;
use prost::Message as _;
use prost_reflect::DescriptorPool;

use super::RegistrySchemaReference;

pub fn protobuf_descriptor_pool(
    definition: &str,
    references: &[RegistrySchemaReference],
) -> anyhow::Result<(DescriptorPool, String)> {
    let root_descriptor = decode_descriptor(definition);
    let root_name = root_descriptor
        .as_ref()
        .and_then(|descriptor| descriptor.name.clone())
        .unwrap_or_else(|| "schema-registry.proto".to_owned());
    let mut definitions = references
        .iter()
        .map(|reference| (reference.name.clone(), reference.definition.clone()))
        .collect::<HashMap<_, _>>();

    if root_descriptor.is_none() {
        anyhow::ensure!(
            definitions
                .insert(root_name.clone(), definition.to_owned())
                .is_none(),
            "Protobuf root schema name conflicts with a referenced schema"
        );
    }

    let mut resolver = protox::file::ChainFileResolver::new();
    resolver.add(MemoryFileResolver { definitions });
    resolver.add(protox::file::GoogleFileResolver::new());
    let mut compiler = protox::Compiler::with_file_resolver(resolver);
    compiler.include_imports(true).include_source_info(false);

    if root_descriptor.is_none() {
        compiler.open_file(&root_name)?;
    } else {
        for reference in references {
            compiler.open_file(&reference.name)?;
        }
    }

    let mut pool = compiler.descriptor_pool();
    if let Some(descriptor) = root_descriptor {
        pool.add_file_descriptor_proto(descriptor)?;
    }
    anyhow::ensure!(
        pool.get_file_by_name(&root_name).is_some(),
        "Protobuf root schema file '{root_name}' is absent from descriptor pool"
    );
    Ok((pool, root_name))
}

fn decode_descriptor(definition: &str) -> Option<prost_reflect::prost_types::FileDescriptorProto> {
    base64::engine::general_purpose::STANDARD
        .decode(definition)
        .ok()
        .and_then(|bytes| {
            prost_reflect::prost_types::FileDescriptorProto::decode(bytes.as_slice()).ok()
        })
}

#[derive(Debug)]
struct MemoryFileResolver {
    definitions: HashMap<String, String>,
}

impl protox::file::FileResolver for MemoryFileResolver {
    fn resolve_path(&self, path: &Path) -> Option<String> {
        path.to_str()
            .filter(|name| self.definitions.contains_key(*name))
            .map(str::to_owned)
    }

    fn open_file(&self, name: &str) -> Result<protox::file::File, protox::Error> {
        let definition = self
            .definitions
            .get(name)
            .ok_or_else(|| protox::Error::file_not_found(name))?;
        decode_descriptor(definition).map_or_else(
            || protox::file::File::from_source(name, definition),
            |descriptor| Ok(protox::file::File::from_file_descriptor_proto(descriptor)),
        )
    }
}
