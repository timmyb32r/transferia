#![allow(
    clippy::tuple_array_conversions,
    clippy::expect_used,
    clippy::items_after_statements,
    clippy::needless_pass_by_ref_mut,
    clippy::match_same_arms,
    clippy::struct_field_names,
    clippy::unnecessary_wraps,
    reason = "protocol parsing checks widths before fixed-size conversions and protobuf wire names intentionally mirror YTsaurus"
)]

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use arrow::buffer::Buffer;
use arrow::ipc::reader::StreamDecoder;
use arrow::record_batch::RecordBatch;
use bytes::{BufMut as _, Bytes, BytesMut};
use prost::Message as _;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinSet;
use uuid::Uuid;

use super::config::{
    YTsaurusAtomicity, YTsaurusPartitionTablesConfig, YTsaurusTableReaderConfig,
};
use super::yt_wire::YtWireDecoder;
use transferia_core::data::schema::DatasetSchema;
use transferia_delivery_contracts::metrics::SinkCounters;
use transferia_delivery_contracts::retry::{jittered_retry_delay, stable_retry_seed};

const BUS_SIGNATURE: u32 = 0x7861_6d4f;
const HANDSHAKE_SIGNATURE: u32 = 0x6873_7562;
const BUS_FIXED_HEADER_BYTES: usize = 36;
const NULL_PART_SIZE: u32 = u32::MAX;
const MAX_PART_SIZE: u32 = 512 * 1024 * 1024;
const BUS_MESSAGE: u16 = 0;
const BUS_ACK: u16 = 1;
const REQUEST_ACKNOWLEDGEMENT: u16 = 1;
const RPC_REQUEST: u32 = 0x6963_7072;
const RPC_RESPONSE: u32 = 0x6f63_7072;
const RPC_STREAMING_PAYLOAD: u32 = 0x7063_7072;
const RPC_STREAMING_FEEDBACK: u32 = 0x6663_7072;
const RPC_STREAM_WINDOW_BYTES: i64 = 16 * 1024 * 1024;
const RPC_TOTAL_STREAM_TIMEOUT_MICROS: i64 = 15 * 60 * 1_000_000;
// YTsaurus patches heavy ReadTable requests to use the full streaming timeout
// for attachment stalls instead of the generic one-minute RPC-proxy default.
const RPC_HEAVY_READ_STALL_TIMEOUT_MICROS: i64 = RPC_TOTAL_STREAM_TIMEOUT_MICROS;
const RPC_PROXY_SELECTION_GRACE: Duration = Duration::from_secs(2);
const RPC_PROXY_PROBE_BYTES: u64 = 64 * 1024 * 1024;
const ROWSET_FORMAT_YT_WIRE: i32 = 0;
const ROWSET_FORMAT_ARROW: i32 = 1;
const TRANSIENT_TABLET_ERROR_CODES: [i32; 21] = [
    1700, // TransactionLockConflict
    1701, // NoSuchTablet
    1702, // TabletNotMounted
    1703, // AllWritesDisabled
    1704, // InvalidMountRevision
    1706, // InvalidTabletState
    1707, // TableMountInfoNotReady
    1712, // RowIsBlocked
    1713, // BlockedRowWaitTimeout
    1720, // BundleResourceLimitExceeded
    1721, // NoSuchCell
    1725, // RequestThrottled
    1732, // SyncReplicaNotInSync
    1735, // ChunkIsNotPreloaded
    1736, // NoInSyncReplicas
    1740, // TabletServantIsNotActive
    1742, // TabletReplicationEraMismatch
    1745, // HunkTabletStoreToggleConflict
    1746, // HunkStoreAllocationFailed
    1747, // TabletResharded
    1748, // ReadOnlySmoothMovementStage
];
pub(super) const PARTITION_MODE_UNORDERED: i32 = 2;
const YSON_STRING: u8 = 0x01;
const YSON_INT64: u8 = 0x02;

// Values are copied verbatim from YTsaurus' CRC64 protocol table. Separators
// would make comparison with the upstream generated table harder to audit.
#[allow(
    clippy::unreadable_literal,
    reason = "values mirror the upstream YTsaurus CRC table verbatim"
)]
static CRC64_TABLE: [u64; 256] = [
    0x0000000000000000_u64,
    0x81789265972743e5_u64,
    0x8389b6aeb968c52f_u64,
    0x02f124cb2e4f86ca_u64,
    0x06136d5d73d18a5f_u64,
    0x876bff38e4f6c9ba_u64,
    0x859adbf3cab94f70_u64,
    0x04e249965d9e0c95_u64,
    0x0c26dabae6a215bf_u64,
    0x8d5e48df7185565a_u64,
    0x8faf6c145fcad090_u64,
    0x0ed7fe71c8ed9375_u64,
    0x0a35b7e795739fe0_u64,
    0x8b4d25820254dc05_u64,
    0x89bc01492c1b5acf_u64,
    0x08c4932cbb3c192a_u64,
    0x993426105a62689b_u64,
    0x184cb475cd452b7e_u64,
    0x1abd90bee30aadb4_u64,
    0x9bc502db742dee51_u64,
    0x9f274b4d29b3e2c4_u64,
    0x1e5fd928be94a121_u64,
    0x1caefde390db27eb_u64,
    0x9dd66f8607fc640e_u64,
    0x9512fcaabcc07d24_u64,
    0x146a6ecf2be73ec1_u64,
    0x169b4a0405a8b80b_u64,
    0x97e3d861928ffbee_u64,
    0x930191f7cf11f77b_u64,
    0x127903925836b49e_u64,
    0x1088275976793254_u64,
    0x91f0b53ce15e71b1_u64,
    0xb311de4523e393d3_u64,
    0x32694c20b4c4d036_u64,
    0x309868eb9a8b56fc_u64,
    0xb1e0fa8e0dac1519_u64,
    0xb502b3185032198c_u64,
    0x347a217dc7155a69_u64,
    0x368b05b6e95adca3_u64,
    0xb7f397d37e7d9f46_u64,
    0xbf3704ffc541866c_u64,
    0x3e4f969a5266c589_u64,
    0x3cbeb2517c294343_u64,
    0xbdc62034eb0e00a6_u64,
    0xb92469a2b6900c33_u64,
    0x385cfbc721b74fd6_u64,
    0x3aaddf0c0ff8c91c_u64,
    0xbbd54d6998df8af9_u64,
    0x2a25f8557981fb48_u64,
    0xab5d6a30eea6b8ad_u64,
    0xa9ac4efbc0e93e67_u64,
    0x28d4dc9e57ce7d82_u64,
    0x2c3695080a507117_u64,
    0xad4e076d9d7732f2_u64,
    0xafbf23a6b338b438_u64,
    0x2ec7b1c3241ff7dd_u64,
    0x260322ef9f23eef7_u64,
    0xa77bb08a0804ad12_u64,
    0xa58a9441264b2bd8_u64,
    0x24f20624b16c683d_u64,
    0x20104fb2ecf264a8_u64,
    0xa168ddd77bd5274d_u64,
    0xa399f91c559aa187_u64,
    0x22e16b79c2bde262_u64,
    0xe75b2eeed1e16442_u64,
    0x6623bc8b46c627a7_u64,
    0x64d298406889a16d_u64,
    0xe5aa0a25ffaee288_u64,
    0xe14843b3a230ee1d_u64,
    0x6030d1d63517adf8_u64,
    0x62c1f51d1b582b32_u64,
    0xe3b967788c7f68d7_u64,
    0xeb7df454374371fd_u64,
    0x6a056631a0643218_u64,
    0x68f442fa8e2bb4d2_u64,
    0xe98cd09f190cf737_u64,
    0xed6e99094492fba2_u64,
    0x6c160b6cd3b5b847_u64,
    0x6ee72fa7fdfa3e8d_u64,
    0xef9fbdc26add7d68_u64,
    0x7e6f08fe8b830cd9_u64,
    0xff179a9b1ca44f3c_u64,
    0xfde6be5032ebc9f6_u64,
    0x7c9e2c35a5cc8a13_u64,
    0x787c65a3f8528686_u64,
    0xf904f7c66f75c563_u64,
    0xfbf5d30d413a43a9_u64,
    0x7a8d4168d61d004c_u64,
    0x7249d2446d211966_u64,
    0xf3314021fa065a83_u64,
    0xf1c064ead449dc49_u64,
    0x70b8f68f436e9fac_u64,
    0x745abf191ef09339_u64,
    0xf5222d7c89d7d0dc_u64,
    0xf7d309b7a7985616_u64,
    0x76ab9bd230bf15f3_u64,
    0x544af0abf202f791_u64,
    0xd53262ce6525b474_u64,
    0xd7c346054b6a32be_u64,
    0x56bbd460dc4d715b_u64,
    0x52599df681d37dce_u64,
    0xd3210f9316f43e2b_u64,
    0xd1d02b5838bbb8e1_u64,
    0x50a8b93daf9cfb04_u64,
    0x586c2a1114a0e22e_u64,
    0xd914b8748387a1cb_u64,
    0xdbe59cbfadc82701_u64,
    0x5a9d0eda3aef64e4_u64,
    0x5e7f474c67716871_u64,
    0xdf07d529f0562b94_u64,
    0xddf6f1e2de19ad5e_u64,
    0x5c8e6387493eeebb_u64,
    0xcd7ed6bba8609f0a_u64,
    0x4c0644de3f47dcef_u64,
    0x4ef7601511085a25_u64,
    0xcf8ff270862f19c0_u64,
    0xcb6dbbe6dbb11555_u64,
    0x4a1529834c9656b0_u64,
    0x48e40d4862d9d07a_u64,
    0xc99c9f2df5fe939f_u64,
    0xc1580c014ec28ab5_u64,
    0x40209e64d9e5c950_u64,
    0x42d1baaff7aa4f9a_u64,
    0xc3a928ca608d0c7f_u64,
    0xc74b615c3d1300ea_u64,
    0x4633f339aa34430f_u64,
    0x44c2d7f2847bc5c5_u64,
    0xc5ba4597135c8620_u64,
    0xceb75cdca3c3c984_u64,
    0x4fcfceb934e48a61_u64,
    0x4d3eea721aab0cab_u64,
    0xcc4678178d8c4f4e_u64,
    0xc8a43181d01243db_u64,
    0x49dca3e44735003e_u64,
    0x4b2d872f697a86f4_u64,
    0xca55154afe5dc511_u64,
    0xc29186664561dc3b_u64,
    0x43e91403d2469fde_u64,
    0x411830c8fc091914_u64,
    0xc060a2ad6b2e5af1_u64,
    0xc482eb3b36b05664_u64,
    0x45fa795ea1971581_u64,
    0x470b5d958fd8934b_u64,
    0xc673cff018ffd0ae_u64,
    0x57837accf9a1a11f_u64,
    0xd6fbe8a96e86e2fa_u64,
    0xd40acc6240c96430_u64,
    0x55725e07d7ee27d5_u64,
    0x519017918a702b40_u64,
    0xd0e885f41d5768a5_u64,
    0xd219a13f3318ee6f_u64,
    0x5361335aa43fad8a_u64,
    0x5ba5a0761f03b4a0_u64,
    0xdadd32138824f745_u64,
    0xd82c16d8a66b718f_u64,
    0x595484bd314c326a_u64,
    0x5db6cd2b6cd23eff_u64,
    0xdcce5f4efbf57d1a_u64,
    0xde3f7b85d5bafbd0_u64,
    0x5f47e9e0429db835_u64,
    0x7da6829980205a57_u64,
    0xfcde10fc170719b2_u64,
    0xfe2f343739489f78_u64,
    0x7f57a652ae6fdc9d_u64,
    0x7bb5efc4f3f1d008_u64,
    0xfacd7da164d693ed_u64,
    0xf83c596a4a991527_u64,
    0x7944cb0fddbe56c2_u64,
    0x7180582366824fe8_u64,
    0xf0f8ca46f1a50c0d_u64,
    0xf209ee8ddfea8ac7_u64,
    0x73717ce848cdc922_u64,
    0x7793357e1553c5b7_u64,
    0xf6eba71b82748652_u64,
    0xf41a83d0ac3b0098_u64,
    0x756211b53b1c437d_u64,
    0xe492a489da4232cc_u64,
    0x65ea36ec4d657129_u64,
    0x671b1227632af7e3_u64,
    0xe6638042f40db406_u64,
    0xe281c9d4a993b893_u64,
    0x63f95bb13eb4fb76_u64,
    0x61087f7a10fb7dbc_u64,
    0xe070ed1f87dc3e59_u64,
    0xe8b47e333ce02773_u64,
    0x69ccec56abc76496_u64,
    0x6b3dc89d8588e25c_u64,
    0xea455af812afa1b9_u64,
    0xeea7136e4f31ad2c_u64,
    0x6fdf810bd816eec9_u64,
    0x6d2ea5c0f6596803_u64,
    0xec5637a5617e2be6_u64,
    0x29ec72327222adc6_u64,
    0xa894e057e505ee23_u64,
    0xaa65c49ccb4a68e9_u64,
    0x2b1d56f95c6d2b0c_u64,
    0x2fff1f6f01f32799_u64,
    0xae878d0a96d4647c_u64,
    0xac76a9c1b89be2b6_u64,
    0x2d0e3ba42fbca153_u64,
    0x25caa8889480b879_u64,
    0xa4b23aed03a7fb9c_u64,
    0xa6431e262de87d56_u64,
    0x273b8c43bacf3eb3_u64,
    0x23d9c5d5e7513226_u64,
    0xa2a157b0707671c3_u64,
    0xa050737b5e39f709_u64,
    0x2128e11ec91eb4ec_u64,
    0xb0d854222840c55d_u64,
    0x31a0c647bf6786b8_u64,
    0x3351e28c91280072_u64,
    0xb22970e9060f4397_u64,
    0xb6cb397f5b914f02_u64,
    0x37b3ab1accb60ce7_u64,
    0x35428fd1e2f98a2d_u64,
    0xb43a1db475dec9c8_u64,
    0xbcfe8e98cee2d0e2_u64,
    0x3d861cfd59c59307_u64,
    0x3f773836778a15cd_u64,
    0xbe0faa53e0ad5628_u64,
    0xbaede3c5bd335abd_u64,
    0x3b9571a02a141958_u64,
    0x3964556b045b9f92_u64,
    0xb81cc70e937cdc77_u64,
    0x9afdac7751c13e15_u64,
    0x1b853e12c6e67df0_u64,
    0x19741ad9e8a9fb3a_u64,
    0x980c88bc7f8eb8df_u64,
    0x9ceec12a2210b44a_u64,
    0x1d96534fb537f7af_u64,
    0x1f6777849b787165_u64,
    0x9e1fe5e10c5f3280_u64,
    0x96db76cdb7632baa_u64,
    0x17a3e4a82044684f_u64,
    0x1552c0630e0bee85_u64,
    0x942a5206992cad60_u64,
    0x90c81b90c4b2a1f5_u64,
    0x11b089f55395e210_u64,
    0x1341ad3e7dda64da_u64,
    0x92393f5beafd273f_u64,
    0x03c98a670ba3568e_u64,
    0x82b118029c84156b_u64,
    0x80403cc9b2cb93a1_u64,
    0x0138aeac25ecd044_u64,
    0x05dae73a7872dcd1_u64,
    0x84a2755fef559f34_u64,
    0x86535194c11a19fe_u64,
    0x072bc3f1563d5a1b_u64,
    0x0fef50dded014331_u64,
    0x8e97c2b87a2600d4_u64,
    0x8c66e6735469861e_u64,
    0x0d1e7416c34ec5fb_u64,
    0x09fc3d809ed0c96e_u64,
    0x8884afe509f78a8b_u64,
    0x8a758b2e27b80c41_u64,
    0x0b0d194bb09f4fa4_u64,
];

static CRC64_SLICING_TABLES: LazyLock<Vec<[u64; 256]>> = LazyLock::new(crc64_slicing_tables);

fn crc64_slicing_tables() -> Vec<[u64; 256]> {
    let mut tables = Vec::with_capacity(16);
    tables.push(CRC64_TABLE);
    while tables.len() < 16 {
        let slice = tables.len();
        let mut table = [0_u64; 256];
        let mut index = 0;
        while index < table.len() {
            let previous = tables[slice - 1][index];
            table[index] = CRC64_TABLE[(previous as u8) as usize] ^ (previous >> 8);
            index += 1;
        }
        tables.push(table);
    }
    tables
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Guid([u32; 4]);

impl Guid {
    fn random() -> Self {
        let bytes = Uuid::new_v4().into_bytes();
        Self([
            u32::from_le_bytes(bytes[0..4].try_into().expect("UUID has four bytes")),
            u32::from_le_bytes(bytes[4..8].try_into().expect("UUID has four bytes")),
            u32::from_le_bytes(bytes[8..12].try_into().expect("UUID has four bytes")),
            u32::from_le_bytes(bytes[12..16].try_into().expect("UUID has four bytes")),
        ])
    }

    const fn handshake() -> Self {
        Self([1, 0, 0, 0])
    }

    fn to_proto(self) -> ProtoGuid {
        ProtoGuid {
            first: u64::from(self.0[0]) | (u64::from(self.0[1]) << 32),
            second: u64::from(self.0[2]) | (u64::from(self.0[3]) << 32),
        }
    }

    fn matches_proto(self, guid: &ProtoGuid) -> bool {
        self.to_proto() == *guid
    }
}

#[derive(Clone, PartialEq, prost::Message)]
struct ProtoGuid {
    #[prost(fixed64, required, tag = "1")]
    first: u64,

    #[prost(fixed64, required, tag = "2")]
    second: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct Handshake {
    #[prost(message, required, tag = "1")]
    connection_id: ProtoGuid,

    #[prost(int32, optional, tag = "3")]
    encryption_mode: Option<i32>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct StreamingParameters {
    #[prost(int64, optional, tag = "1")]
    window_size: Option<i64>,

    #[prost(int64, optional, tag = "2")]
    read_timeout: Option<i64>,

    #[prost(int64, optional, tag = "3")]
    write_timeout: Option<i64>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct Credentials {
    #[prost(string, optional, tag = "2")]
    pub(super) token: Option<String>,
}

pub(super) fn credentials(token: &str) -> Credentials {
    Credentials {
        token: Some(token.to_owned()),
    }
}

#[derive(Clone, PartialEq, prost::Message)]
struct RequestHeader {
    #[prost(message, optional, tag = "1")]
    request_id: Option<ProtoGuid>,

    #[prost(string, required, tag = "2")]
    service: String,

    #[prost(string, required, tag = "3")]
    method: String,

    #[prost(int32, optional, tag = "9")]
    protocol_version_major: Option<i32>,

    #[prost(int64, optional, tag = "10")]
    timeout: Option<i64>,

    #[prost(string, optional, tag = "18")]
    user_agent: Option<String>,

    #[prost(int32, optional, tag = "23")]
    request_codec: Option<i32>,

    #[prost(int32, optional, tag = "24")]
    response_codec: Option<i32>,

    #[prost(message, optional, tag = "33")]
    server_attachments_streaming_parameters: Option<StreamingParameters>,

    #[prost(message, optional, tag = "110")]
    credentials: Option<Credentials>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ReadTableRequest {
    #[prost(bytes = "vec", required, tag = "1")]
    path: Vec<u8>,

    #[prost(bool, optional, tag = "2")]
    unordered: Option<bool>,

    #[prost(bytes = "vec", optional, tag = "4")]
    config: Option<Vec<u8>>,

    #[prost(int32, optional, tag = "8")]
    desired_rowset_format: Option<i32>,

    #[prost(int32, optional, tag = "10")]
    arrow_fallback_rowset_format: Option<i32>,

    #[prost(bool, optional, tag = "11")]
    enable_any_unpacking: Option<bool>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct PartitionTablesRequest {
    #[prost(bytes = "vec", repeated, tag = "1")]
    paths: Vec<Vec<u8>>,

    #[prost(int32, required, tag = "5")]
    partition_mode: i32,

    #[prost(int64, optional, tag = "14")]
    compressed_data_size_per_partition: Option<i64>,

    #[prost(int32, optional, tag = "7")]
    max_partition_count: Option<i32>,

    #[prost(bool, optional, tag = "8")]
    enable_key_guarantee: Option<bool>,

    #[prost(bool, optional, tag = "9")]
    adjust_data_weight_per_partition: Option<bool>,

    #[prost(bool, optional, tag = "10")]
    enable_cookies: Option<bool>,

    #[prost(bool, optional, tag = "15")]
    fetch_cookie_node_descriptors: Option<bool>,

    #[prost(bool, optional, tag = "13")]
    omit_inaccessible_rows: Option<bool>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct PartitionStatistics {
    #[prost(int64, optional, tag = "1")]
    chunk_count: Option<i64>,

    #[prost(int64, optional, tag = "2")]
    data_weight: Option<i64>,

    #[prost(int64, optional, tag = "3")]
    row_count: Option<i64>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct MultiTablePartition {
    #[prost(bytes = "vec", repeated, tag = "1")]
    table_ranges: Vec<Vec<u8>>,

    #[prost(message, optional, tag = "2")]
    aggregate_statistics: Option<PartitionStatistics>,

    #[prost(bytes = "vec", optional, tag = "3")]
    cookie: Option<Vec<u8>>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct PartitionTablesResponse {
    #[prost(message, repeated, tag = "1")]
    partitions: Vec<MultiTablePartition>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct DataStatistics {
    #[prost(int64, optional, tag = "3")]
    row_count: Option<i64>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RowsetStatistics {
    #[prost(int64, required, tag = "1")]
    total_row_count: i64,

    #[prost(message, required, tag = "2")]
    data_statistics: DataStatistics,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ReadTablePartitionRequest {
    #[prost(bytes = "vec", optional, tag = "1")]
    cookie: Option<Vec<u8>>,

    #[prost(bool, optional, tag = "2")]
    unordered: Option<bool>,

    #[prost(bytes = "vec", optional, tag = "4")]
    config: Option<Vec<u8>>,

    #[prost(int32, optional, tag = "8")]
    desired_rowset_format: Option<i32>,

    #[prost(int32, optional, tag = "10")]
    arrow_fallback_rowset_format: Option<i32>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct StartTransactionRequest {
    #[prost(int32, required, tag = "1")]
    transaction_type: i32,

    #[prost(int64, optional, tag = "2")]
    timeout: Option<i64>,

    #[prost(bool, optional, tag = "6")]
    sticky: Option<bool>,

    #[prost(bool, optional, tag = "7")]
    ping: Option<bool>,

    #[prost(int32, optional, tag = "9")]
    atomicity: Option<i32>,

    #[prost(int32, optional, tag = "10")]
    durability: Option<i32>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct StartTransactionResponse {
    #[prost(message, required, tag = "1")]
    id: ProtoGuid,

    #[prost(uint64, required, tag = "2")]
    start_timestamp: u64,

    #[prost(int64, optional, tag = "3")]
    sequence_number_source_id: Option<i64>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ModifyRowsRequest {
    #[prost(int64, optional, tag = "6")]
    sequence_number: Option<i64>,

    #[prost(int64, optional, tag = "9")]
    sequence_number_source_id: Option<i64>,

    #[prost(message, required, tag = "1")]
    transaction_id: ProtoGuid,

    #[prost(bytes = "vec", required, tag = "2")]
    path: Vec<u8>,

    #[prost(int32, repeated, tag = "3")]
    row_modification_types: Vec<i32>,

    #[prost(bool, optional, tag = "4")]
    require_sync_replica: Option<bool>,

    #[prost(message, required, tag = "200")]
    rowset_descriptor: RowsetDescriptor,
}

#[derive(Clone, PartialEq, prost::Message)]
struct CommitTransactionRequest {
    #[prost(message, required, tag = "1")]
    transaction_id: ProtoGuid,
}

#[derive(Clone, PartialEq, prost::Message)]
struct AbortTransactionRequest {
    #[prost(message, required, tag = "1")]
    transaction_id: ProtoGuid,
}

#[derive(Clone, PartialEq, prost::Message)]
struct EmptyResponse {}

#[derive(Clone, PartialEq, prost::Message)]
struct ProtoError {
    #[prost(int32, required, tag = "1")]
    code: i32,

    #[prost(string, optional, tag = "2")]
    message: Option<String>,

    #[prost(message, repeated, tag = "4")]
    inner_errors: Vec<Self>,
}

impl ProtoError {
    fn format_chain(&self) -> String {
        let mut messages = Vec::new();
        self.collect_messages(&mut messages);
        if messages.is_empty() {
            format!("YTsaurus RPC error {}", self.code)
        } else {
            format!("YTsaurus RPC error {}: {}", self.code, messages.join(": "))
        }
    }

    fn collect_messages(&self, output: &mut Vec<String>) {
        if let Some(message) = self
            .message
            .as_deref()
            .filter(|message| !message.is_empty())
        {
            output.push(message.to_owned());
        }
        for inner in &self.inner_errors {
            inner.collect_messages(output);
        }
    }

    fn contains_transient_dynamic_write_code(&self) -> bool {
        is_transient_dynamic_write_error_code(self.code)
            || self
                .inner_errors
                .iter()
                .any(Self::contains_transient_dynamic_write_code)
    }
}

#[derive(Debug)]
struct NativeRpcError(ProtoError);

impl std::fmt::Display for NativeRpcError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0.format_chain())
    }
}

impl std::error::Error for NativeRpcError {}

#[derive(Clone, PartialEq, prost::Message)]
struct ResponseHeader {
    #[prost(message, optional, tag = "1")]
    request_id: Option<ProtoGuid>,

    #[prost(message, optional, tag = "2")]
    error: Option<ProtoError>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct StreamingPayloadHeader {
    #[prost(message, required, tag = "1")]
    request_id: ProtoGuid,

    #[prost(string, required, tag = "2")]
    service: String,

    #[prost(string, required, tag = "3")]
    method: String,

    #[prost(int32, required, tag = "5")]
    sequence_number: i32,

    #[prost(int32, optional, tag = "6")]
    codec: Option<i32>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct StreamingFeedbackHeader {
    #[prost(message, required, tag = "1")]
    request_id: ProtoGuid,

    #[prost(string, required, tag = "2")]
    service: String,

    #[prost(string, required, tag = "3")]
    method: String,

    #[prost(int64, required, tag = "5")]
    read_position: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RowsetDescriptor {
    #[prost(int32, optional, tag = "1")]
    wire_format_version: Option<i32>,

    #[prost(int32, optional, tag = "2")]
    rowset_kind: Option<i32>,

    #[prost(int32, optional, tag = "4")]
    rowset_format: Option<i32>,

    #[prost(message, repeated, tag = "3")]
    name_table_entries: Vec<NameTableEntry>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RowsetFormatDescriptor {
    #[prost(int32, optional, tag = "4")]
    rowset_format: Option<i32>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct NameTableEntry {
    #[prost(string, optional, tag = "1")]
    name: Option<String>,
}

struct Packet {
    packet_type: u16,
    flags: u16,
    id: Guid,
    parts: Vec<Option<Bytes>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum NativeReadFormat {
    Arrow,
    YtWire,
}

impl NativeReadFormat {
    const fn rowset_format(self) -> i32 {
        match self {
            Self::Arrow => ROWSET_FORMAT_ARROW,
            Self::YtWire => ROWSET_FORMAT_YT_WIRE,
        }
    }

    pub(super) const fn name(self) -> &'static str {
        match self {
            Self::Arrow => "arrow",
            Self::YtWire => "yt_wire",
        }
    }
}

pub(super) enum NativeReadPayload {
    Encoded(Bytes),
    Decoded(Vec<RecordBatch>),
}

pub(super) struct NativeDynamicWriter {
    workers: Vec<Mutex<NativeDynamicWorker>>,
    next_worker: AtomicUsize,
}

impl NativeDynamicWriter {
    pub(super) fn new(
        endpoints: Vec<String>,
        token: String,
        atomicity: YTsaurusAtomicity,
        concurrency: usize,
        transaction_timeout: Duration,
        retry_initial: Duration,
        retry_max: Duration,
        counters: Arc<SinkCounters>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !endpoints.is_empty(),
            "YTsaurus dynamic writer requires at least one RPC proxy endpoint"
        );
        anyhow::ensure!(
            concurrency > 0,
            "YTsaurus dynamic writer concurrency must be positive"
        );
        let workers = (0..concurrency)
            .map(|index| {
                Mutex::new(NativeDynamicWorker {
                    endpoints: endpoints.clone(),
                    token: token.clone(),
                    atomicity,
                    next_endpoint: index % endpoints.len(),
                    transaction_timeout,
                    retry_initial,
                    retry_max,
                    retry_seed: stable_retry_seed(&index.to_le_bytes()),
                    counters: Arc::clone(&counters),
                    stream: None,
                })
            })
            .collect();
        Ok(Self {
            workers,
            next_worker: AtomicUsize::new(0),
        })
    }

    pub(super) async fn write_rows(
        &self,
        path: &str,
        row_count: usize,
        column_names: &[String],
        payload: Bytes,
        require_sync_replica: bool,
    ) -> anyhow::Result<()> {
        if row_count == 0 {
            return Ok(());
        }
        let worker_index = self.next_worker.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        self.workers[worker_index]
            .lock()
            .await
            .write_rows(
                path,
                row_count,
                column_names,
                payload,
                require_sync_replica,
            )
            .await
    }
}

struct NativeDynamicWorker {
    endpoints: Vec<String>,
    token: String,
    atomicity: YTsaurusAtomicity,
    next_endpoint: usize,
    transaction_timeout: Duration,
    retry_initial: Duration,
    retry_max: Duration,
    retry_seed: u64,
    counters: Arc<SinkCounters>,
    stream: Option<TcpStream>,
}

impl NativeDynamicWorker {
    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.stream.is_some() {
            return Ok(());
        }
        let mut failures = Vec::new();
        for offset in 0..self.endpoints.len() {
            let index = (self.next_endpoint + offset) % self.endpoints.len();
            let endpoint = &self.endpoints[index];
            match TcpStream::connect(endpoint).await {
                Ok(mut stream) => {
                    stream.set_nodelay(true)?;
                    if let Err(error) = perform_handshake(&mut stream).await {
                        failures.push(format!("{endpoint}: {error:#}"));
                        continue;
                    }
                    self.next_endpoint = (index + 1) % self.endpoints.len();
                    self.stream = Some(stream);
                    return Ok(());
                }
                Err(error) => failures.push(format!("{endpoint}: {error}")),
            }
        }
        anyhow::bail!(
            "all YTsaurus RPC endpoints rejected the dynamic writer connection: {}",
            failures.join("; ")
        )
    }

    async fn write_rows(
        &mut self,
        path: &str,
        row_count: usize,
        column_names: &[String],
        payload: Bytes,
        require_sync_replica: bool,
    ) -> anyhow::Result<()> {
        let mut attempt = 0_u32;
        let mut delay = self.retry_initial;
        loop {
            self.connect().await?;
            let result = self
                .write_rows_on_stream(
                    path,
                    row_count,
                    column_names,
                    payload.clone(),
                    require_sync_replica,
                )
                .await;
            match result {
                Ok(()) => return Ok(()),
                Err(error) => {
                    self.stream = None;
                    if !is_transient_dynamic_write_error(&error) {
                        return Err(error);
                    }
                    self.counters.add_retries(1);
                    let sleep = jittered_retry_delay(delay, attempt, self.retry_seed);
                    tracing::warn!(
                        retry_delay_ms = sleep.as_millis(),
                        error = ?error,
                        "YTsaurus dynamic write was throttled or rebalanced; retrying"
                    );
                    tokio::time::sleep(sleep).await;
                    attempt = attempt.saturating_add(1);
                    delay = delay.saturating_mul(2).min(self.retry_max);
                }
            }
        }
    }

    async fn write_rows_on_stream(
        &mut self,
        path: &str,
        row_count: usize,
        column_names: &[String],
        payload: Bytes,
        require_sync_replica: bool,
    ) -> anyhow::Result<()> {
        let stream = self
            .stream
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("YTsaurus dynamic writer has no RPC connection"))?;
        let timeout_micros = i64::try_from(self.transaction_timeout.as_micros())?;
        let start = StartTransactionRequest {
            transaction_type: 1,
            timeout: Some(timeout_micros),
            sticky: Some(true),
            // Tablet transactions require proxy-side pings. Omitting the field
            // preserves the protocol default (`true`) and keeps the short
            // write transaction alive until commit.
            ping: None,
            atomicity: Some(self.atomicity.rpc_value()),
            durability: Some(0),
        };
        let response = invoke_unary_on_stream::<StartTransactionResponse>(
            stream,
            &self.token,
            "StartTransaction",
            Bytes::from(start.encode_to_vec()),
            &[],
        )
        .await?;
        let transaction_id = response.id;
        let descriptor = RowsetDescriptor {
            wire_format_version: Some(1),
            rowset_kind: Some(1),
            rowset_format: Some(ROWSET_FORMAT_YT_WIRE),
            name_table_entries: column_names
                .iter()
                .map(|name| NameTableEntry {
                    name: Some(name.clone()),
                })
                .collect(),
        };
        let modify = ModifyRowsRequest {
            sequence_number: Some(0),
            sequence_number_source_id: response.sequence_number_source_id,
            transaction_id: transaction_id.clone(),
            path: path.as_bytes().to_vec(),
            row_modification_types: vec![0; row_count],
            require_sync_replica: Some(require_sync_replica),
            rowset_descriptor: descriptor,
        };
        if let Err(error) = invoke_unary_on_stream::<EmptyResponse>(
            stream,
            &self.token,
            "ModifyRows",
            Bytes::from(modify.encode_to_vec()),
            &[payload],
        )
        .await
        {
            let abort = AbortTransactionRequest {
                transaction_id: transaction_id.clone(),
            };
            let _abort_result = invoke_unary_on_stream::<EmptyResponse>(
                stream,
                &self.token,
                "AbortTransaction",
                Bytes::from(abort.encode_to_vec()),
                &[],
            )
            .await;
            return Err(error).context("YTsaurus ModifyRows failed");
        }
        let commit = CommitTransactionRequest { transaction_id };
        invoke_unary_on_stream::<EmptyResponse>(
            stream,
            &self.token,
            "CommitTransaction",
            Bytes::from(commit.encode_to_vec()),
            &[],
        )
        .await
        .context("YTsaurus tablet transaction commit failed")?;
        Ok(())
    }
}

pub(super) fn is_transient_dynamic_write_error(error: &anyhow::Error) -> bool {
    // These protocol error codes describe temporary tablet state, bundle
    // pressure, or a concurrent mount/reshard transition. YTsaurus uses them
    // as backpressure: the exact same idempotent upsert transaction must be
    // retried after a bounded delay rather than failing the delivery.
    if error
        .downcast_ref::<NativeRpcError>()
        .is_some_and(|rpc| rpc.0.contains_transient_dynamic_write_code())
    {
        return true;
    }
    let message = format!("{error:#}").to_ascii_lowercase();
    [
        "cannot mount table since node is locked by mount-unmount operation",
        "node is out of tablet memory",
        "too many overlapping stores in tablet",
        "active store is overflown",
        "dynamic store pool size limit reached",
        "too many stores in tablet",
        "too many dynamic stores in tablet",
        "is not in \"mounted\" state",
        "no such tablet",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}

pub(super) fn is_transient_dynamic_write_error_code(code: i32) -> bool {
    TRANSIENT_TABLET_ERROR_CODES.contains(&code)
}

pub(super) struct NativeReadBlock {
    pub payload: NativeReadPayload,
    pub network_raw_bytes: u64,
    pub network_decoded_bytes: u64,
    pub network_decode_duration: Duration,
    pub format: NativeReadFormat,
    pub name_table_entries: Vec<String>,
    pub stream_id: Option<usize>,
    pub end_of_stream: bool,
    pub cumulative_rows: Option<u64>,
}

pub(super) struct NativePipelinedReadStream {
    receiver: mpsc::Receiver<anyhow::Result<NativeReadBlock>>,
    tasks: JoinSet<()>,
}

impl NativePipelinedReadStream {
    pub(super) fn new(stream: NativeReadStream, decode_arrow: bool) -> Self {
        let (sender, receiver) = mpsc::channel(2);
        let mut tasks = JoinSet::new();
        tasks.spawn(run_native_read_pipeline(stream, sender, decode_arrow));
        Self { receiver, tasks }
    }

    pub(super) async fn next_block(&mut self) -> anyhow::Result<Option<NativeReadBlock>> {
        receive_read_worker_item(
            &mut self.receiver,
            &mut self.tasks,
            "YTsaurus native Arrow reader",
        )
        .await
    }
}

impl Drop for NativePipelinedReadStream {
    fn drop(&mut self) {
        self.tasks.abort_all();
    }
}

async fn run_native_read_pipeline(
    mut stream: NativeReadStream,
    sender: mpsc::Sender<anyhow::Result<NativeReadBlock>>,
    decode_arrow: bool,
) {
    let (read_sender, mut read_receiver) = mpsc::channel(2);
    let read_task = tokio::spawn(async move {
        loop {
            match stream.next_block().await {
                Ok(Some(block)) => {
                    if read_sender.send(Ok(block)).await.is_err() {
                        return;
                    }
                }
                Ok(None) => return,
                Err(error) => {
                    drop(read_sender.send(Err(error)).await);
                    return;
                }
            }
        }
    });
    let mut decoder = decode_arrow.then(StreamDecoder::new);
    while let Some(block) = read_receiver.recv().await {
        let mut block = match block {
            Ok(block) => block,
            Err(error) => {
                drop(sender.send(Err(error)).await);
                return;
            }
        };
        if let Some(current_decoder) = decoder.take() {
            let NativeReadPayload::Encoded(payload) = block.payload else {
                read_task.abort();
                drop(
                    sender
                        .send(Err(anyhow::anyhow!(
                            "YTsaurus native Arrow reader received an already-decoded rowset"
                        )))
                        .await,
                );
                return;
            };
            let decoded = tokio::task::spawn_blocking(move || {
                let started = Instant::now();
                let mut decoder = current_decoder;
                let batches = decode_arrow_bytes(&mut decoder, payload)?;
                Ok::<_, anyhow::Error>((decoder, batches, started.elapsed()))
            })
            .await;
            let (next_decoder, batches, decode_duration) = match decoded {
                Ok(Ok(decoded)) => decoded,
                Ok(Err(error)) => {
                    read_task.abort();
                    drop(sender.send(Err(error)).await);
                    return;
                }
                Err(error) => {
                    read_task.abort();
                    drop(
                        sender
                            .send(Err(anyhow::anyhow!(
                                "YTsaurus Arrow decode worker failed: {error}"
                            )))
                            .await,
                    );
                    return;
                }
            };
            decoder = Some(next_decoder);
            block.payload = NativeReadPayload::Decoded(batches);
            block.network_decode_duration = decode_duration;
        }
        if sender.send(Ok(block)).await.is_err() {
            read_task.abort();
            return;
        }
    }
    if let Err(error) = read_task.await {
        drop(
            sender
                .send(Err(anyhow::anyhow!(
                    "YTsaurus native network reader failed: {error}"
                )))
                .await,
        );
        return;
    }
    if let Some(mut decoder) = decoder {
        if let Err(error) = decoder.finish() {
            drop(sender.send(Err(error.into())).await);
        }
    }
}

pub(super) struct NativePartition {
    pub cookie: Bytes,
    pub row_count: Option<u64>,
    pub data_weight: Option<i64>,
}

pub(super) struct NativePartitionedReadStream {
    receiver: mpsc::Receiver<anyhow::Result<NativeReadBlock>>,
    tasks: JoinSet<()>,
    queued: Option<NativeReadBlock>,
}

impl NativePartitionedReadStream {
    pub(super) async fn open(
        endpoints: &[String],
        token: &str,
        path: &str,
        partition_config: YTsaurusPartitionTablesConfig,
        table_reader: &YTsaurusTableReaderConfig,
        requested_format: NativeReadFormat,
        decode_arrow: bool,
        yt_wire_schema: Option<DatasetSchema>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            yt_wire_schema.is_none() || requested_format == NativeReadFormat::YtWire,
            "YT wire decoder schema requires the YT wire rowset format"
        );
        anyhow::ensure!(
            !decode_arrow || yt_wire_schema.is_none(),
            "Arrow and YT wire decoders cannot be enabled together"
        );
        let partitions = partition_tables(endpoints, token, path, partition_config).await?;
        anyhow::ensure!(
            !partitions.is_empty(),
            "YTsaurus PartitionTables returned no partitions for '{path}'"
        );
        let expected_rows = partitions
            .iter()
            .filter_map(|partition| partition.row_count)
            .try_fold(0_u64, u64::checked_add);
        let expected_data_weight = partitions
            .iter()
            .filter_map(|partition| partition.data_weight)
            .try_fold(0_i64, i64::checked_add);
        let concurrency = partition_config.concurrency.min(partitions.len());
        tracing::info!(
            table_path = path,
            partition_count = partitions.len(),
            concurrency,
            expected_rows = ?expected_rows,
            expected_data_weight = ?expected_data_weight,
            "YTsaurus PartitionTables plan created"
        );

        let endpoints = Arc::new(endpoints.to_vec());
        let token: Arc<str> = Arc::from(token);
        let table_reader = Arc::new(table_reader.clone());
        let yt_wire_schema = yt_wire_schema.map(Arc::new);
        let failed = Arc::new(AtomicBool::new(false));
        let channel_capacity = concurrency.saturating_mul(2).max(1);
        let (sender, receiver) = mpsc::channel(channel_capacity);
        let mut tasks = JoinSet::new();
        for worker in 0..concurrency {
            let worker_partitions = partitions
                .iter()
                .enumerate()
                .skip(worker)
                .step_by(concurrency)
                .map(|(index, partition)| (index, partition.cookie.clone(), partition.row_count))
                .collect::<Vec<_>>();
            let endpoints = Arc::clone(&endpoints);
            let token = Arc::clone(&token);
            let table_reader = Arc::clone(&table_reader);
            let yt_wire_schema = yt_wire_schema.clone();
            let failed = Arc::clone(&failed);
            let sender = sender.clone();
            tasks.spawn(async move {
                for (partition_index, cookie, row_count) in worker_partitions {
                    if failed.load(Ordering::Acquire) {
                        return;
                    }
                    let mut stream = match NativeReadStream::open_partition(
                        &endpoints,
                        &token,
                        cookie,
                        &table_reader,
                        requested_format,
                    )
                    .await
                    {
                        Ok(stream) => stream,
                        Err(error) => {
                            publish_partition_failure(&failed, &sender, error).await;
                            return;
                        }
                    };
                    let (read_sender, mut read_receiver) = mpsc::channel(2);
                    let read_task = tokio::spawn(async move {
                        loop {
                            match stream.next_block().await {
                                Ok(Some(block)) => {
                                    if read_sender.send(Ok(block)).await.is_err() {
                                        return;
                                    }
                                }
                                Ok(None) => return,
                                Err(error) => {
                                    drop(read_sender.send(Err(error)).await);
                                    return;
                                }
                            }
                        }
                    });
                    let mut arrow_decoder = decode_arrow.then(StreamDecoder::new);
                    let mut yt_wire_decoder = yt_wire_schema
                        .as_deref()
                        .map(YtWireDecoder::new);
                    while let Some(block) = read_receiver.recv().await {
                        let mut block = match block {
                            Ok(block) => block,
                            Err(error) => {
                                publish_partition_failure(&failed, &sender, error).await;
                                return;
                            }
                        };
                        block.stream_id = Some(partition_index);
                        if let Some(decoder) = arrow_decoder.take() {
                            let NativeReadPayload::Encoded(payload) = block.payload else {
                                read_task.abort();
                                publish_partition_failure(
                                    &failed,
                                    &sender,
                                    anyhow::anyhow!(
                                        "YTsaurus partition worker received an already-decoded rowset"
                                    ),
                                )
                                .await;
                                return;
                            };
                            let decoded = tokio::task::spawn_blocking(move || {
                                let started = Instant::now();
                                let mut decoder = decoder;
                                let batches = decode_arrow_bytes(&mut decoder, payload)?;
                                Ok::<_, anyhow::Error>((decoder, batches, started.elapsed()))
                            })
                            .await;
                            let (decoder, batches, decode_duration) = match decoded {
                                Ok(Ok(decoded)) => decoded,
                                Ok(Err(error)) => {
                                    read_task.abort();
                                    publish_partition_failure(&failed, &sender, error).await;
                                    return;
                                }
                                Err(error) => {
                                    read_task.abort();
                                    publish_partition_failure(
                                        &failed,
                                        &sender,
                                        anyhow::anyhow!(
                                            "YTsaurus Arrow decode worker failed: {error}"
                                        ),
                                    )
                                    .await;
                                    return;
                                }
                            };
                            arrow_decoder = Some(decoder);
                            block.payload = NativeReadPayload::Decoded(batches);
                            block.network_decode_duration = decode_duration;
                        } else if let Some(decoder) = yt_wire_decoder.take() {
                            let NativeReadPayload::Encoded(payload) = block.payload else {
                                read_task.abort();
                                publish_partition_failure(
                                    &failed,
                                    &sender,
                                    anyhow::anyhow!(
                                        "YTsaurus partition worker received an already-decoded YT wire rowset"
                                    ),
                                )
                                .await;
                                return;
                            };
                            let name_table_entries =
                                std::mem::take(&mut block.name_table_entries);
                            let decoded = tokio::task::spawn_blocking(move || {
                                let started = Instant::now();
                                let mut decoder = decoder;
                                let batch = decoder.decode(&name_table_entries, payload)?;
                                Ok::<_, anyhow::Error>((decoder, batch, started.elapsed()))
                            })
                            .await;
                            let (decoder, batch, decode_duration) = match decoded {
                                Ok(Ok(decoded)) => decoded,
                                Ok(Err(error)) => {
                                    read_task.abort();
                                    publish_partition_failure(&failed, &sender, error).await;
                                    return;
                                }
                                Err(error) => {
                                    read_task.abort();
                                    publish_partition_failure(
                                        &failed,
                                        &sender,
                                        anyhow::anyhow!(
                                            "YTsaurus YT wire decode worker failed: {error}"
                                        ),
                                    )
                                    .await;
                                    return;
                                }
                            };
                            yt_wire_decoder = Some(decoder);
                            block.payload = NativeReadPayload::Decoded(vec![batch]);
                            block.network_decode_duration = decode_duration;
                        }
                        if sender.send(Ok(block)).await.is_err() {
                            read_task.abort();
                            return;
                        }
                    }
                    if let Err(error) = read_task.await {
                        publish_partition_failure(
                            &failed,
                            &sender,
                            anyhow::anyhow!(
                                "YTsaurus partition network reader failed: {error}"
                            ),
                        )
                        .await;
                        return;
                    }
                    if let Some(mut decoder) = arrow_decoder.take() {
                        if let Err(error) = decoder.finish() {
                            publish_partition_failure(&failed, &sender, error.into()).await;
                            return;
                        }
                    }
                    let end = NativeReadBlock {
                        payload: if decode_arrow || yt_wire_schema.is_some() {
                            NativeReadPayload::Decoded(Vec::new())
                        } else {
                            NativeReadPayload::Encoded(Bytes::new())
                        },
                        network_raw_bytes: 0,
                        network_decoded_bytes: 0,
                        network_decode_duration: Duration::ZERO,
                        format: requested_format,
                        name_table_entries: Vec::new(),
                        stream_id: Some(partition_index),
                        end_of_stream: true,
                        cumulative_rows: row_count,
                    };
                    if sender.send(Ok(end)).await.is_err() {
                        return;
                    }
                }
            });
        }
        drop(sender);
        let mut stream = Self {
            receiver,
            tasks,
            queued: None,
        };
        stream.queued = Some(stream.receive_block().await?.ok_or_else(|| {
            anyhow::anyhow!("YTsaurus PartitionTables readers ended before returning a rowset")
        })?);
        Ok(stream)
    }

    pub(super) async fn next_block(&mut self) -> anyhow::Result<Option<NativeReadBlock>> {
        if let Some(block) = self.queued.take() {
            return Ok(Some(block));
        }
        self.receive_block().await
    }

    async fn receive_block(&mut self) -> anyhow::Result<Option<NativeReadBlock>> {
        receive_read_worker_item(
            &mut self.receiver,
            &mut self.tasks,
            "YTsaurus PartitionTables reader",
        )
        .await
    }
}

impl Drop for NativePartitionedReadStream {
    fn drop(&mut self) {
        self.tasks.abort_all();
    }
}

async fn publish_partition_failure(
    failed: &AtomicBool,
    sender: &mpsc::Sender<anyhow::Result<NativeReadBlock>>,
    error: anyhow::Error,
) {
    if failed
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        drop(sender.send(Err(error)).await);
    }
}

pub(super) async fn receive_read_worker_item<T>(
    receiver: &mut mpsc::Receiver<anyhow::Result<T>>,
    tasks: &mut JoinSet<()>,
    reader: &str,
) -> anyhow::Result<Option<T>> {
    loop {
        tokio::select! {
            item = receiver.recv() => match item {
                Some(Ok(item)) => return Ok(Some(item)),
                Some(Err(error)) => {
                    tasks.abort_all();
                    return Err(error);
                }
                None => {
                    while let Some(result) = tasks.join_next().await {
                        result.map_err(|error| anyhow::anyhow!(
                            "{reader} worker failed before completing its partitions: {error}"
                        ))?;
                    }
                    return Ok(None);
                }
            },
            result = tasks.join_next(), if !tasks.is_empty() => match result {
                Some(Ok(())) => {}
                Some(Err(error)) => {
                    tasks.abort_all();
                    return Err(anyhow::anyhow!(
                        "{reader} worker failed before completing its partitions: {error}"
                    ));
                }
                None => {}
            }
        }
    }
}

pub(super) async fn partition_tables(
    endpoints: &[String],
    token: &str,
    path: &str,
    config: YTsaurusPartitionTablesConfig,
) -> anyhow::Result<Vec<NativePartition>> {
    anyhow::ensure!(!endpoints.is_empty(), "YTsaurus RPC endpoint list is empty");
    let request = PartitionTablesRequest {
        paths: vec![binary_rich_read_path(path, 0)?],
        partition_mode: PARTITION_MODE_UNORDERED,
        compressed_data_size_per_partition: Some(i64::try_from(
            config.compressed_data_size_per_partition,
        )?),
        max_partition_count: Some(i32::try_from(config.max_partition_count)?),
        enable_key_guarantee: Some(false),
        adjust_data_weight_per_partition: Some(true),
        enable_cookies: Some(true),
        fetch_cookie_node_descriptors: Some(true),
        omit_inaccessible_rows: Some(false),
    };
    let request = Bytes::from(request.encode_to_vec());
    let start = usize::try_from(Guid::random().0[0])? % endpoints.len();
    let mut failures = Vec::new();
    for offset in 0..endpoints.len() {
        let endpoint = &endpoints[(start + offset) % endpoints.len()];
        match invoke_unary::<PartitionTablesResponse>(
            endpoint,
            token,
            "PartitionTables",
            request.clone(),
        )
        .await
        {
            Ok(response) => {
                return response
                    .partitions
                    .into_iter()
                    .enumerate()
                    .map(|(index, partition)| {
                        let statistics = partition.aggregate_statistics.unwrap_or_default();
                        let cookie = partition.cookie.ok_or_else(|| {
                            anyhow::anyhow!(
                                "YTsaurus PartitionTables response partition {index} has no cookie"
                            )
                        })?;
                        anyhow::ensure!(
                            !cookie.is_empty(),
                            "YTsaurus PartitionTables response partition {index} has an empty cookie"
                        );
                        let row_count = statistics
                            .row_count
                            .map(|row_count| {
                                u64::try_from(row_count).map_err(|_| {
                                    anyhow::anyhow!(
                                        "YTsaurus partition {index} has negative row_count {row_count}"
                                    )
                                })
                            })
                            .transpose()?
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "YTsaurus PartitionTables response partition {index} has no row_count"
                                )
                            })?;
                        Ok(NativePartition {
                            cookie: Bytes::from(cookie),
                            row_count: Some(row_count),
                            data_weight: statistics.data_weight,
                        })
                    })
                    .collect();
            }
            Err(error) => failures.push(format!("{endpoint}: {error:#}")),
        }
    }
    anyhow::bail!(
        "all YTsaurus RPC endpoints rejected PartitionTables: {}",
        failures.join("; ")
    )
}

async fn invoke_unary<Response>(
    endpoint: &str,
    token: &str,
    method: &'static str,
    request_body: Bytes,
) -> anyhow::Result<Response>
where
    Response: prost::Message + Default,
{
    let (body, attachments) =
        invoke_unary_raw(endpoint, token, "ApiService", 1, method, request_body).await?;
    anyhow::ensure!(
        attachments.is_empty(),
        "YTsaurus unary {method} unexpectedly returned {} attachments",
        attachments.len()
    );
    Response::decode(body).map_err(Into::into)
}

async fn invoke_unary_raw(
    endpoint: &str,
    token: &str,
    service: &'static str,
    protocol_version: i32,
    method: &'static str,
    request_body: Bytes,
) -> anyhow::Result<(Bytes, Vec<Option<Bytes>>)> {
    let mut stream = TcpStream::connect(endpoint).await?;
    stream.set_nodelay(true)?;
    perform_handshake(&mut stream).await?;
    invoke_unary_raw_on_stream(
        &mut stream,
        token,
        service,
        protocol_version,
        method,
        request_body,
    )
    .await
}

async fn invoke_unary_raw_on_stream(
    stream: &mut TcpStream,
    token: &str,
    service: &'static str,
    protocol_version: i32,
    method: &'static str,
    request_body: Bytes,
) -> anyhow::Result<(Bytes, Vec<Option<Bytes>>)> {
    invoke_unary_raw_with_attachments_on_stream(
        stream,
        token,
        service,
        protocol_version,
        method,
        request_body,
        &[],
    )
    .await
}

async fn invoke_unary_on_stream<Response>(
    stream: &mut TcpStream,
    token: &str,
    method: &'static str,
    request_body: Bytes,
    request_attachments: &[Bytes],
) -> anyhow::Result<Response>
where
    Response: prost::Message + Default,
{
    let (body, attachments) = invoke_unary_raw_with_attachments_on_stream(
        stream,
        token,
        "ApiService",
        1,
        method,
        request_body,
        request_attachments,
    )
    .await?;
    anyhow::ensure!(
        attachments.is_empty(),
        "YTsaurus unary {method} unexpectedly returned {} attachments",
        attachments.len()
    );
    Response::decode(body).map_err(Into::into)
}

async fn invoke_unary_raw_with_attachments_on_stream(
    stream: &mut TcpStream,
    token: &str,
    service: &'static str,
    protocol_version: i32,
    method: &'static str,
    request_body: Bytes,
    request_attachments: &[Bytes],
) -> anyhow::Result<(Bytes, Vec<Option<Bytes>>)> {
    let request_id = Guid::random();
    let header = request_header(request_id, token, service, protocol_version, method, false);
    let mut parts = Vec::with_capacity(2 + request_attachments.len());
    parts.push(Some(proto_part(RPC_REQUEST, &header)?));
    parts.push(Some(request_body));
    parts.extend(request_attachments.iter().cloned().map(Some));
    write_packet(stream, BUS_MESSAGE, 0, request_id, &parts).await?;

    loop {
        let packet = read_packet(stream).await?;
        if packet.flags & REQUEST_ACKNOWLEDGEMENT != 0 {
            write_packet(stream, BUS_ACK, 0, packet.id, &[]).await?;
        }
        if packet.packet_type == BUS_ACK {
            continue;
        }
        anyhow::ensure!(
            packet.packet_type == BUS_MESSAGE,
            "YTsaurus Bus returned unsupported packet type {}",
            packet.packet_type
        );
        let Some(Some(header_part)) = packet.parts.first() else {
            anyhow::bail!("YTsaurus RPC returned a packet without a message header");
        };
        anyhow::ensure!(
            header_part.len() >= 4,
            "YTsaurus RPC message header is shorter than its type"
        );
        let message_type =
            u32::from_le_bytes(header_part[..4].try_into().expect("four checked bytes"));
        anyhow::ensure!(
            message_type == RPC_RESPONSE,
            "YTsaurus unary {method} returned unsupported message type {message_type:#x}"
        );
        let header = ResponseHeader::decode(&header_part[4..])?;
        if let Some(response_request_id) = header.request_id {
            anyhow::ensure!(
                request_id.matches_proto(&response_request_id),
                "YTsaurus unary {method} response belongs to another request"
            );
        }
        if let Some(error) = header.error.filter(|error| error.code != 0) {
            return Err(NativeRpcError(error).into());
        }
        let mut parts = packet.parts.into_iter();
        parts.next();
        let body = parts.next().flatten().ok_or_else(|| {
            anyhow::anyhow!("YTsaurus unary {service}.{method} response has no body")
        })?;
        return Ok((body, parts.collect()));
    }
}

fn request_header(
    request_id: Guid,
    token: &str,
    service: &'static str,
    protocol_version: i32,
    method: &'static str,
    streaming: bool,
) -> RequestHeader {
    RequestHeader {
        request_id: Some(request_id.to_proto()),
        service: service.to_owned(),
        method: method.to_owned(),
        protocol_version_major: Some(protocol_version),
        timeout: Some(RPC_TOTAL_STREAM_TIMEOUT_MICROS),
        user_agent: Some("Transferia Rust native YTsaurus reader".to_owned()),
        request_codec: Some(0),
        response_codec: Some(0),
        server_attachments_streaming_parameters: streaming.then_some(StreamingParameters {
            window_size: Some(RPC_STREAM_WINDOW_BYTES),
            read_timeout: Some(RPC_HEAVY_READ_STALL_TIMEOUT_MICROS),
            write_timeout: Some(RPC_HEAVY_READ_STALL_TIMEOUT_MICROS),
        }),
        credentials: Some(credentials(token)),
    }
}

pub(super) struct NativeReadStream {
    reader: OwnedReadHalf,
    writer: mpsc::Sender<NativeWriteCommand>,
    writer_errors: mpsc::Receiver<anyhow::Error>,
    request_id: Guid,
    method: &'static str,
    next_sequence_number: i32,
    read_position: i64,
    metadata_received: bool,
    finished: bool,
    queued: VecDeque<NativeReadBlock>,
    packets_received: u64,
    requested_format: NativeReadFormat,
    with_statistics: bool,
}

struct NativeWriteCommand {
    packet_type: u16,
    flags: u16,
    id: Guid,
    parts: Vec<Option<Bytes>>,
}

impl NativeReadStream {
    pub(super) async fn open(
        endpoints: &[String],
        token: &str,
        path: &str,
        start_row_index: i64,
        unordered: bool,
        table_reader: &YTsaurusTableReaderConfig,
        requested_format: NativeReadFormat,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(!endpoints.is_empty(), "YTsaurus RPC endpoint list is empty");
        const SPECULATIVE_OPEN_FANOUT: usize = 8;
        let start = usize::try_from(Guid::random().0[0])? % endpoints.len();
        let fanout = endpoints.len().min(SPECULATIVE_OPEN_FANOUT);
        let mut attempts = tokio::task::JoinSet::new();
        for offset in 0..fanout {
            let endpoint = endpoints[(start + offset) % endpoints.len()].clone();
            let token = token.to_owned();
            let path = path.to_owned();
            let table_reader = table_reader.clone();
            attempts.spawn(async move {
                let started = Instant::now();
                let result = Self::open_at(
                    &endpoint,
                    &token,
                    &path,
                    start_row_index,
                    unordered,
                    &table_reader,
                    requested_format,
                )
                .await;
                (endpoint, started.elapsed(), result)
            });
        }
        let mut failures = Vec::new();
        let mut opened = Vec::new();
        loop {
            let attempt = if opened.is_empty() {
                attempts.join_next().await
            } else {
                match tokio::time::timeout(RPC_PROXY_SELECTION_GRACE, attempts.join_next()).await {
                    Ok(attempt) => attempt,
                    Err(_) => {
                        attempts.abort_all();
                        break;
                    }
                }
            };
            let Some(attempt) = attempt else {
                break;
            };
            match attempt {
                Ok((endpoint, elapsed, Ok(stream))) => {
                    opened.push((endpoint, elapsed, stream));
                }
                Ok((endpoint, _, Err(error))) => {
                    failures.push(format!("{endpoint}: {error:#}"));
                }
                Err(error) => failures.push(format!("YTsaurus RPC open task failed: {error}")),
            }
        }
        let mut probes = JoinSet::new();
        for (endpoint, open_elapsed, stream) in opened {
            probes.spawn(async move {
                let result = stream.probe_throughput(RPC_PROXY_PROBE_BYTES).await;
                (endpoint, open_elapsed, result)
            });
        }
        let mut best = None;
        while let Some(probe) = probes.join_next().await {
            match probe {
                Ok((endpoint, open_elapsed, Ok((stream, bytes, probe_elapsed)))) => {
                    let seconds = probe_elapsed.as_secs_f64().max(f64::EPSILON);
                    let bytes_per_second = bytes as f64 / seconds;
                    tracing::info!(
                        endpoint,
                        open_elapsed_ms = open_elapsed.as_millis(),
                        probe_elapsed_ms = probe_elapsed.as_millis(),
                        probe_bytes = bytes,
                        probe_mib_per_second = bytes_per_second / 1024.0 / 1024.0,
                        "YTsaurus native RPC proxy candidate measured"
                    );
                    if best
                        .as_ref()
                        .is_none_or(|(_, _, best_rate): &(_, _, f64)| bytes_per_second > *best_rate)
                    {
                        best = Some((endpoint, stream, bytes_per_second));
                    }
                }
                Ok((endpoint, _, Err(error))) => {
                    failures.push(format!("{endpoint}: throughput probe failed: {error:#}"));
                }
                Err(error) => failures.push(format!("YTsaurus RPC probe task failed: {error}")),
            }
        }
        if let Some((endpoint, stream, bytes_per_second)) = best {
            tracing::info!(
                endpoint,
                speculative_fanout = fanout,
                probe_mib_per_second = bytes_per_second / 1024.0 / 1024.0,
                "YTsaurus native row stream opened"
            );
            return Ok(stream);
        }
        anyhow::bail!(
            "all YTsaurus RPC endpoints rejected the native row stream: {}",
            failures.join("; ")
        )
    }

    async fn probe_throughput(
        mut self,
        target_bytes: u64,
    ) -> anyhow::Result<(Self, u64, Duration)> {
        let started = Instant::now();
        let mut bytes = 0_u64;
        let mut sampled = Vec::new();
        while bytes < target_bytes {
            let Some(block) = self.next_block().await? else {
                break;
            };
            bytes = bytes
                .checked_add(block.network_raw_bytes)
                .ok_or_else(|| anyhow::anyhow!("YTsaurus proxy probe byte count overflow"))?;
            let end_of_stream = block.end_of_stream;
            sampled.push(block);
            if end_of_stream {
                break;
            }
        }
        anyhow::ensure!(
            bytes > 0,
            "YTsaurus RPC proxy probe returned no rowset bytes"
        );
        for block in sampled.into_iter().rev() {
            self.queued.push_front(block);
        }
        Ok((self, bytes, started.elapsed()))
    }

    async fn open_at(
        endpoint: &str,
        token: &str,
        path: &str,
        start_row_index: i64,
        unordered: bool,
        table_reader: &YTsaurusTableReaderConfig,
        requested_format: NativeReadFormat,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            start_row_index >= 0,
            "YTsaurus start row index must not be negative"
        );
        let table_reader_yson = table_reader.to_yson();
        let request = ReadTableRequest {
            path: binary_rich_read_path(path, start_row_index)?,
            unordered: Some(unordered),
            config: (table_reader_yson != "{}").then(|| table_reader_yson.into_bytes()),
            desired_rowset_format: Some(requested_format.rowset_format()),
            // Let the server report that Arrow is unavailable via the rowset
            // descriptor. `arrow_payload` rejects that descriptor before any
            // data enters the pipeline, so fallback is explicit and fail-closed.
            arrow_fallback_rowset_format: Some(ROWSET_FORMAT_YT_WIRE),
            enable_any_unpacking: Some(true),
        };
        let stream = Self::open_encoded_at(
            endpoint,
            token,
            "ReadTable",
            Bytes::from(request.encode_to_vec()),
            requested_format,
            true,
        )
        .await?;
        tracing::info!(
            endpoint,
            unordered,
            start_row_index,
            "YTsaurus native ReadTable request sent"
        );
        Ok(stream)
    }

    async fn open_partition(
        endpoints: &[String],
        token: &str,
        cookie: Bytes,
        table_reader: &YTsaurusTableReaderConfig,
        requested_format: NativeReadFormat,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(!endpoints.is_empty(), "YTsaurus RPC endpoint list is empty");
        let start = usize::try_from(Guid::random().0[0])? % endpoints.len();
        let mut failures = Vec::new();
        for offset in 0..endpoints.len() {
            let endpoint = &endpoints[(start + offset) % endpoints.len()];
            match Self::open_partition_at(
                endpoint,
                token,
                cookie.clone(),
                table_reader,
                requested_format,
            )
            .await
            {
                Ok(stream) => return Ok(stream),
                Err(error) => failures.push(format!("{endpoint}: {error:#}")),
            }
        }
        anyhow::bail!(
            "all YTsaurus RPC endpoints rejected ReadTablePartition: {}",
            failures.join("; ")
        )
    }

    async fn open_partition_at(
        endpoint: &str,
        token: &str,
        cookie: Bytes,
        table_reader: &YTsaurusTableReaderConfig,
        requested_format: NativeReadFormat,
    ) -> anyhow::Result<Self> {
        let table_reader_yson = table_reader.to_yson();
        let request = ReadTablePartitionRequest {
            cookie: Some(cookie.to_vec()),
            unordered: Some(true),
            config: (table_reader_yson != "{}").then(|| table_reader_yson.into_bytes()),
            desired_rowset_format: Some(requested_format.rowset_format()),
            arrow_fallback_rowset_format: Some(ROWSET_FORMAT_YT_WIRE),
        };
        let stream = Self::open_encoded_at(
            endpoint,
            token,
            "ReadTablePartition",
            Bytes::from(request.encode_to_vec()),
            requested_format,
            false,
        )
        .await?;
        tracing::debug!(endpoint, "YTsaurus native ReadTablePartition request sent");
        Ok(stream)
    }

    async fn open_encoded_at(
        endpoint: &str,
        token: &str,
        method: &'static str,
        request_body: Bytes,
        requested_format: NativeReadFormat,
        with_statistics: bool,
    ) -> anyhow::Result<Self> {
        let mut stream = TcpStream::connect(endpoint).await?;
        stream.set_nodelay(true)?;
        perform_handshake(&mut stream).await?;
        tracing::debug!(endpoint, method, "YTsaurus native RPC handshake completed");

        let request_id = Guid::random();
        let header = request_header(request_id, token, "ApiService", 1, method, true);
        let parts = [Some(proto_part(RPC_REQUEST, &header)?), Some(request_body)];
        write_packet(&mut stream, BUS_MESSAGE, 0, request_id, &parts).await?;
        close_request_attachments_stream(&mut stream, request_id, method).await?;

        let (reader, writer) = stream.into_split();
        let (write_sender, write_receiver) = mpsc::channel(8);
        let (writer_error_sender, writer_errors) = mpsc::channel(1);
        tokio::spawn(run_native_writer(
            writer,
            write_receiver,
            writer_error_sender,
        ));
        let mut stream = Self {
            reader,
            writer: write_sender,
            writer_errors,
            request_id,
            method,
            next_sequence_number: 0,
            read_position: 0,
            metadata_received: false,
            finished: false,
            queued: VecDeque::new(),
            packets_received: 0,
            requested_format,
            with_statistics,
        };
        // Planning can take tens of seconds for a very large static table. Keep
        // that phase under the caller's stream-open timeout and expose a stream
        // only after metadata and the first data block have arrived.
        if let Some(first) = stream.next_block().await? {
            stream.queued.push_front(first);
        }
        Ok(stream)
    }

    pub(super) async fn next_block(&mut self) -> anyhow::Result<Option<NativeReadBlock>> {
        if let Some(block) = self.queued.pop_front() {
            return Ok(Some(block));
        }
        if self.finished {
            return Ok(None);
        }

        loop {
            let packet = tokio::select! {
                packet = read_packet(&mut self.reader) => packet?,
                writer_error = self.writer_errors.recv() => {
                    return Err(writer_error.unwrap_or_else(|| {
                        anyhow::anyhow!("YTsaurus native RPC writer stopped unexpectedly")
                    }));
                }
            };
            self.packets_received = self.packets_received.saturating_add(1);
            if self.packets_received <= 4 {
                tracing::trace!(
                    packet_index = self.packets_received,
                    packet_type = packet.packet_type,
                    packet_flags = packet.flags,
                    part_count = packet.parts.len(),
                    "YTsaurus native RPC packet received"
                );
            }
            if packet.flags & REQUEST_ACKNOWLEDGEMENT != 0 {
                self.send_packet(BUS_ACK, 0, packet.id, Vec::new()).await?;
            }
            if packet.packet_type == BUS_ACK {
                continue;
            }
            anyhow::ensure!(
                packet.packet_type == BUS_MESSAGE,
                "YTsaurus Bus returned unsupported packet type {}",
                packet.packet_type
            );
            let Some(Some(header_part)) = packet.parts.first() else {
                anyhow::bail!("YTsaurus RPC returned a packet without a message header");
            };
            anyhow::ensure!(
                header_part.len() >= 4,
                "YTsaurus RPC message header is shorter than its type"
            );
            let message_type =
                u32::from_le_bytes(header_part[..4].try_into().expect("four checked bytes"));
            if self.packets_received <= 4 {
                tracing::trace!(
                    packet_index = self.packets_received,
                    message_type,
                    "YTsaurus native RPC message received"
                );
            }
            match message_type {
                RPC_STREAMING_PAYLOAD => {
                    self.handle_payload(header_part, &packet.parts[1..]).await?;
                    if let Some(block) = self.queued.pop_front() {
                        return Ok(Some(block));
                    }
                    if self.finished {
                        return Ok(None);
                    }
                }
                RPC_STREAMING_FEEDBACK => self.handle_server_feedback(header_part)?,
                RPC_RESPONSE => self.handle_response(header_part)?,
                other => anyhow::bail!("YTsaurus RPC returned unsupported message type {other:#x}"),
            }
        }
    }

    fn handle_server_feedback(&self, header_part: &Bytes) -> anyhow::Result<()> {
        let feedback = StreamingFeedbackHeader::decode(&header_part[4..])?;
        anyhow::ensure!(
            self.request_id.matches_proto(&feedback.request_id),
            "YTsaurus RPC streaming feedback belongs to another request"
        );
        anyhow::ensure!(
            feedback.service == "ApiService" && feedback.method == self.method,
            "YTsaurus RPC streaming feedback belongs to {}.{}",
            feedback.service,
            feedback.method
        );
        Ok(())
    }

    async fn handle_payload(
        &mut self,
        header_part: &Bytes,
        attachments: &[Option<Bytes>],
    ) -> anyhow::Result<()> {
        let header = StreamingPayloadHeader::decode(&header_part[4..])?;
        anyhow::ensure!(
            self.request_id.matches_proto(&header.request_id),
            "YTsaurus RPC streaming payload belongs to another request"
        );
        anyhow::ensure!(
            header.service == "ApiService" && header.method == self.method,
            "YTsaurus RPC streaming payload belongs to {}.{}",
            header.service,
            header.method
        );
        anyhow::ensure!(
            header.codec.unwrap_or(0) == 0,
            "YTsaurus RPC streaming codec {} is unsupported",
            header.codec.unwrap_or(0)
        );
        anyhow::ensure!(
            header.sequence_number == self.next_sequence_number,
            "YTsaurus RPC streaming sequence jumped from {} to {}",
            self.next_sequence_number,
            header.sequence_number
        );
        self.next_sequence_number = self
            .next_sequence_number
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("YTsaurus RPC streaming sequence overflow"))?;
        anyhow::ensure!(
            !attachments.is_empty(),
            "YTsaurus RPC streaming payload has no attachments"
        );

        for attachment in attachments {
            let compressed_size = attachment
                .as_ref()
                .map_or(1_usize, |bytes| bytes.len().max(1));
            self.read_position = self
                .read_position
                .checked_add(i64::try_from(compressed_size)?)
                .ok_or_else(|| anyhow::anyhow!("YTsaurus RPC feedback position overflow"))?;
            if !self.metadata_received {
                anyhow::ensure!(
                    attachment.is_some(),
                    "YTsaurus RPC stream ended before table metadata"
                );
                self.metadata_received = true;
                continue;
            }
            let Some(block) = attachment else {
                self.finished = true;
                continue;
            };
            if let Some(payload) =
                rowset_payload(block, self.requested_format, self.with_statistics)?
            {
                self.queued.push_back(NativeReadBlock {
                    network_raw_bytes: u64::try_from(block.len())?,
                    network_decoded_bytes: u64::try_from(payload.payload.len())?,
                    network_decode_duration: Duration::ZERO,
                    payload: NativeReadPayload::Encoded(payload.payload),
                    format: payload.format,
                    name_table_entries: payload.name_table_entries,
                    stream_id: None,
                    end_of_stream: false,
                    cumulative_rows: payload.cumulative_rows,
                });
            }
        }
        self.send_feedback().await
    }

    fn handle_response(&self, header_part: &Bytes) -> anyhow::Result<()> {
        let header = ResponseHeader::decode(&header_part[4..])?;
        if let Some(request_id) = header.request_id {
            anyhow::ensure!(
                self.request_id.matches_proto(&request_id),
                "YTsaurus RPC response belongs to another request"
            );
        }
        if let Some(error) = header.error.filter(|error| error.code != 0) {
            return Err(NativeRpcError(error).into());
        }
        Ok(())
    }

    async fn send_feedback(&mut self) -> anyhow::Result<()> {
        let feedback = StreamingFeedbackHeader {
            request_id: self.request_id.to_proto(),
            service: "ApiService".to_owned(),
            method: self.method.to_owned(),
            read_position: self.read_position,
        };
        let parts = vec![Some(proto_part(RPC_STREAMING_FEEDBACK, &feedback)?)];
        self.send_packet(BUS_MESSAGE, REQUEST_ACKNOWLEDGEMENT, Guid::random(), parts)
            .await
    }

    async fn send_packet(
        &mut self,
        packet_type: u16,
        flags: u16,
        id: Guid,
        parts: Vec<Option<Bytes>>,
    ) -> anyhow::Result<()> {
        self.writer
            .send(NativeWriteCommand {
                packet_type,
                flags,
                id,
                parts,
            })
            .await
            .map_err(|_| anyhow::anyhow!("YTsaurus native RPC writer stopped unexpectedly"))
    }
}

async fn run_native_writer(
    mut writer: OwnedWriteHalf,
    mut commands: mpsc::Receiver<NativeWriteCommand>,
    errors: mpsc::Sender<anyhow::Error>,
) {
    while let Some(command) = commands.recv().await {
        if let Err(error) = write_packet(
            &mut writer,
            command.packet_type,
            command.flags,
            command.id,
            &command.parts,
        )
        .await
        {
            drop(errors.send(error).await);
            return;
        }
    }
}

pub(super) fn decode_arrow_bytes(
    decoder: &mut StreamDecoder,
    bytes: Bytes,
) -> anyhow::Result<Vec<RecordBatch>> {
    let mut buffer = Buffer::from(bytes);
    let mut batches = Vec::new();
    while !buffer.is_empty() {
        match decoder.decode(&mut buffer) {
            Ok(Some(batch)) => batches.push(batch),
            Ok(None) => {}
            Err(arrow::error::ArrowError::IpcError(message)) if message == "Unexpected EOS" => {
                *decoder = StreamDecoder::new();
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(batches)
}

async fn close_request_attachments_stream(
    stream: &mut TcpStream,
    request_id: Guid,
    method: &'static str,
) -> anyhow::Result<()> {
    let header = StreamingPayloadHeader {
        request_id: request_id.to_proto(),
        service: "ApiService".to_owned(),
        method: method.to_owned(),
        sequence_number: 0,
        codec: Some(0),
    };
    let parts = [Some(proto_part(RPC_STREAMING_PAYLOAD, &header)?), None];
    write_packet(
        stream,
        BUS_MESSAGE,
        REQUEST_ACKNOWLEDGEMENT,
        Guid::random(),
        &parts,
    )
    .await
}

fn binary_rich_read_path(path: &str, start_row_index: i64) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(!path.is_empty(), "YTsaurus read path must not be empty");
    let mut output = Vec::with_capacity(path.len() + 96);
    if start_row_index != 0 {
        output.push(b'<');
        binary_yson_string(&mut output, "ranges")?;
        output.push(b'=');
        output.push(b'[');
        output.push(b'{');
        binary_yson_string(&mut output, "lower_limit")?;
        output.push(b'=');
        output.push(b'{');
        binary_yson_string(&mut output, "row_index")?;
        output.push(b'=');
        output.push(YSON_INT64);
        write_var_uint(&mut output, zig_zag_i64(start_row_index));
        output.extend_from_slice(b";};};]");
        output.push(b';');
        output.push(b'>');
    }
    // RichYPath encodes only its attribute dictionary as YSON. The Cypress
    // path itself follows it verbatim; encoding the path as a YSON string
    // makes the leading string marker part of the path name on the server.
    output.extend_from_slice(path.as_bytes());
    Ok(output)
}

fn binary_yson_string(output: &mut Vec<u8>, value: &str) -> anyhow::Result<()> {
    output.push(YSON_STRING);
    let length = i32::try_from(value.len())?;
    write_var_uint(output, u64::from(zig_zag_i32(length)));
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

const fn zig_zag_i32(value: i32) -> u32 {
    ((value << 1) ^ (value >> 31)) as u32
}

const fn zig_zag_i64(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

fn write_var_uint(output: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        output.push((value as u8) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn proto_part(message_type: u32, message: &impl prost::Message) -> anyhow::Result<Bytes> {
    let encoded = message.encode_to_vec();
    let mut bytes = BytesMut::with_capacity(4 + encoded.len());
    bytes.extend_from_slice(&message_type.to_le_bytes());
    bytes.extend_from_slice(&encoded);
    Ok(bytes.freeze())
}

async fn perform_handshake(stream: &mut TcpStream) -> anyhow::Result<()> {
    let handshake = Handshake {
        connection_id: Guid::random().to_proto(),
        encryption_mode: Some(1),
    };
    let mut part = BytesMut::with_capacity(4 + handshake.encoded_len());
    part.extend_from_slice(&HANDSHAKE_SIGNATURE.to_le_bytes());
    handshake.encode(&mut part)?;
    write_packet(
        stream,
        BUS_MESSAGE,
        0,
        Guid::handshake(),
        &[Some(part.freeze())],
    )
    .await?;

    let response = read_packet(stream).await?;
    anyhow::ensure!(
        response.packet_type == BUS_MESSAGE && response.id == Guid::handshake(),
        "YTsaurus Bus returned an invalid handshake packet"
    );
    let data = response
        .parts
        .first()
        .and_then(Option::as_ref)
        .ok_or_else(|| anyhow::anyhow!("YTsaurus Bus handshake has no payload"))?;
    anyhow::ensure!(
        data.len() >= 4
            && u32::from_le_bytes(data[..4].try_into().expect("four checked bytes"))
                == HANDSHAKE_SIGNATURE,
        "YTsaurus Bus returned an invalid handshake signature"
    );
    let handshake = Handshake::decode(&data[4..])?;
    anyhow::ensure!(
        handshake.encryption_mode.unwrap_or(0) != 2,
        "YTsaurus RPC proxy requires encrypted Bus transport, which this connection did not negotiate"
    );
    Ok(())
}

async fn read_packet(stream: &mut (impl AsyncRead + Unpin)) -> anyhow::Result<Packet> {
    let mut fixed = [0_u8; BUS_FIXED_HEADER_BYTES];
    stream.read_exact(&mut fixed).await?;
    anyhow::ensure!(
        u32::from_le_bytes(fixed[0..4].try_into().expect("four fixed bytes")) == BUS_SIGNATURE,
        "YTsaurus Bus packet signature mismatch"
    );
    let expected_fixed_crc =
        u64::from_le_bytes(fixed[28..36].try_into().expect("eight fixed bytes"));
    anyhow::ensure!(
        checksum_matches(expected_fixed_crc, &fixed[..28]),
        "YTsaurus Bus fixed-header checksum mismatch"
    );
    let packet_type = u16::from_le_bytes(fixed[4..6].try_into().expect("two fixed bytes"));
    let flags = u16::from_le_bytes(fixed[6..8].try_into().expect("two fixed bytes"));
    let id = Guid([
        u32::from_le_bytes(fixed[8..12].try_into().expect("four fixed bytes")),
        u32::from_le_bytes(fixed[12..16].try_into().expect("four fixed bytes")),
        u32::from_le_bytes(fixed[16..20].try_into().expect("four fixed bytes")),
        u32::from_le_bytes(fixed[20..24].try_into().expect("four fixed bytes")),
    ]);
    let part_count = usize::try_from(u32::from_le_bytes(
        fixed[24..28].try_into().expect("four fixed bytes"),
    ))?;
    if packet_type != BUS_MESSAGE && part_count == 0 {
        return Ok(Packet {
            packet_type,
            flags,
            id,
            parts: Vec::new(),
        });
    }
    anyhow::ensure!(
        part_count <= 1 << 20,
        "YTsaurus Bus packet declares too many parts: {part_count}"
    );
    let variable_bytes = part_count
        .checked_mul(12)
        .and_then(|bytes| bytes.checked_add(8))
        .ok_or_else(|| anyhow::anyhow!("YTsaurus Bus variable-header length overflow"))?;
    let mut variable = vec![0_u8; variable_bytes];
    stream.read_exact(&mut variable).await?;
    let expected_variable_crc = u64::from_le_bytes(
        variable[variable_bytes - 8..]
            .try_into()
            .expect("eight variable bytes"),
    );
    anyhow::ensure!(
        checksum_matches(expected_variable_crc, &variable[..variable_bytes - 8]),
        "YTsaurus Bus variable-header checksum mismatch"
    );

    let mut parts = Vec::with_capacity(part_count);
    for index in 0..part_count {
        let size = u32::from_le_bytes(
            variable[index * 4..index * 4 + 4]
                .try_into()
                .expect("four variable bytes"),
        );
        if size == NULL_PART_SIZE {
            parts.push(None);
            continue;
        }
        anyhow::ensure!(
            size <= MAX_PART_SIZE,
            "YTsaurus Bus message part is too large: {size}"
        );
        let size = usize::try_from(size)?;
        let mut bytes = BytesMut::with_capacity(size);
        while bytes.len() < size {
            // `BytesMut` may reserve more than requested. Restrict each read to
            // this Bus part so the socket cannot consume bytes belonging to
            // the next part while still avoiding a zero-filled allocation.
            let remaining = size - bytes.len();
            let mut limited = (&mut bytes).limit(remaining);
            anyhow::ensure!(
                stream.read_buf(&mut limited).await? != 0,
                "YTsaurus Bus message part ended after {} of {size} bytes",
                bytes.len(),
            );
        }
        let checksum_offset = part_count * 4 + index * 8;
        let expected = u64::from_le_bytes(
            variable[checksum_offset..checksum_offset + 8]
                .try_into()
                .expect("eight variable bytes"),
        );
        anyhow::ensure!(
            checksum_matches(expected, &bytes),
            "YTsaurus Bus part checksum mismatch"
        );
        parts.push(Some(bytes.freeze()));
    }
    Ok(Packet {
        packet_type,
        flags,
        id,
        parts,
    })
}

async fn write_packet(
    stream: &mut (impl AsyncWrite + Unpin),
    packet_type: u16,
    flags: u16,
    id: Guid,
    parts: &[Option<Bytes>],
) -> anyhow::Result<()> {
    let mut fixed = [0_u8; BUS_FIXED_HEADER_BYTES];
    fixed[0..4].copy_from_slice(&BUS_SIGNATURE.to_le_bytes());
    fixed[4..6].copy_from_slice(&packet_type.to_le_bytes());
    fixed[6..8].copy_from_slice(&flags.to_le_bytes());
    for (index, value) in id.0.into_iter().enumerate() {
        let offset = 8 + index * 4;
        fixed[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }
    fixed[24..28].copy_from_slice(&u32::try_from(parts.len())?.to_le_bytes());
    let checksum = crc64(&fixed[..28]);
    fixed[28..36].copy_from_slice(&checksum.to_le_bytes());
    stream.write_all(&fixed).await?;

    if packet_type != BUS_MESSAGE && parts.is_empty() {
        return Ok(());
    }
    let variable_bytes = parts
        .len()
        .checked_mul(12)
        .and_then(|bytes| bytes.checked_add(8))
        .ok_or_else(|| anyhow::anyhow!("YTsaurus Bus variable-header length overflow"))?;
    let mut variable = vec![0_u8; variable_bytes];
    for (index, part) in parts.iter().enumerate() {
        let size = part
            .as_ref()
            .map_or(Ok(NULL_PART_SIZE), |bytes| u32::try_from(bytes.len()))?;
        variable[index * 4..index * 4 + 4].copy_from_slice(&size.to_le_bytes());
        let checksum_offset = parts.len() * 4 + index * 8;
        variable[checksum_offset..checksum_offset + 8]
            .copy_from_slice(&crc64(part.as_deref().unwrap_or_default()).to_le_bytes());
    }
    let checksum = crc64(&variable[..variable_bytes - 8]);
    variable[variable_bytes - 8..].copy_from_slice(&checksum.to_le_bytes());
    stream.write_all(&variable).await?;
    for part in parts.iter().flatten() {
        stream.write_all(part).await?;
    }
    Ok(())
}

pub(super) struct RowsetPayload {
    pub(super) payload: Bytes,
    pub(super) format: NativeReadFormat,
    pub(super) name_table_entries: Vec<String>,
    pub(super) cumulative_rows: Option<u64>,
}

pub(super) fn rowset_payload(
    block: &Bytes,
    requested_format: NativeReadFormat,
    with_statistics: bool,
) -> anyhow::Result<Option<RowsetPayload>> {
    let (rows, cumulative_rows) = if with_statistics {
        let (rows, statistics) = unpack_two_refs(
            block,
            "YTsaurus rowset block envelope must contain data and statistics",
        )?;
        let statistics = RowsetStatistics::decode(statistics)?;
        let cumulative_rows = statistics
            .data_statistics
            .row_count
            .map(|row_count| {
                u64::try_from(row_count).map_err(|_| {
                    anyhow::anyhow!("YTsaurus rowset statistics has negative row_count {row_count}")
                })
            })
            .transpose()?
            .ok_or_else(|| anyhow::anyhow!("YTsaurus rowset statistics has no row_count"))?;
        (rows, Some(cumulative_rows))
    } else {
        (block.clone(), None)
    };
    let (descriptor, payload) =
        unpack_two_refs(&rows, "YTsaurus rowset must contain descriptor and payload")?;
    // Arrow does not use YT's name table. Decode only the rowset-format tag on
    // that hot path so prost skips the repeated names without allocating a
    // `String` for every column in every streamed block.
    let rowset_format = RowsetFormatDescriptor::decode(descriptor.clone())?
        .rowset_format
        .unwrap_or(ROWSET_FORMAT_YT_WIRE);
    let format = match rowset_format {
        ROWSET_FORMAT_ARROW => NativeReadFormat::Arrow,
        ROWSET_FORMAT_YT_WIRE => NativeReadFormat::YtWire,
        other => anyhow::bail!("YTsaurus returned unsupported rowset format {other}"),
    };
    if requested_format == NativeReadFormat::Arrow && format != NativeReadFormat::Arrow {
        // The RPC proxy encodes one final empty batch before closing every
        // ReadTable stream. Arrow's encoder deliberately represents that
        // zero-row terminator in YT wire format, even when every data batch was
        // Arrow. It is control framing, not a data-format fallback. Accept only
        // its exact empty-row encoding; any non-empty YT wire rowset remains a
        // fail-closed violation of the physical-columnar contract.
        if payload.as_ref() == 0_u64.to_le_bytes() {
            return Ok(None);
        }
        anyhow::bail!(
            "YTsaurus metadata reported that every physical chunk is table_unversioned_columnar, but ReadTable returned {} instead of Arrow; refusing the fallback",
            format.name()
        );
    }
    anyhow::ensure!(
        format == requested_format,
        "YTsaurus returned {} rowsets after {} was selected",
        format.name(),
        requested_format.name()
    );
    let name_table_entries = if format == NativeReadFormat::YtWire {
        RowsetDescriptor::decode(descriptor)?
            .name_table_entries
            .into_iter()
            .map(|entry| {
                entry
                    .name
                    .ok_or_else(|| anyhow::anyhow!("YTsaurus rowset name-table entry has no name"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?
    } else {
        Vec::new()
    };
    Ok(Some(RowsetPayload {
        payload,
        format,
        name_table_entries,
        cumulative_rows,
    }))
}

fn unpack_two_refs(bytes: &Bytes, message: &'static str) -> anyhow::Result<(Bytes, Bytes)> {
    anyhow::ensure!(bytes.len() >= 4, "YTsaurus packed refs header is truncated");
    let count = i32::from_le_bytes(bytes[..4].try_into().expect("four checked bytes"));
    anyhow::ensure!(count == 2, "{message}");
    let mut offset = 4_usize;
    let mut refs = [Bytes::new(), Bytes::new()];
    for item in &mut refs {
        let length_end = offset
            .checked_add(8)
            .ok_or_else(|| anyhow::anyhow!("YTsaurus packed ref length offset overflow"))?;
        anyhow::ensure!(
            length_end <= bytes.len(),
            "YTsaurus packed refs lengths are truncated"
        );
        let length = i64::from_le_bytes(
            bytes[offset..length_end]
                .try_into()
                .expect("eight checked bytes"),
        );
        anyhow::ensure!(length >= 0, "YTsaurus packed ref length is negative");
        let length = usize::try_from(length)?;
        let end = length_end
            .checked_add(length)
            .ok_or_else(|| anyhow::anyhow!("YTsaurus packed ref offset overflow"))?;
        anyhow::ensure!(end <= bytes.len(), "YTsaurus packed ref is truncated");
        *item = bytes.slice(length_end..end);
        offset = end;
    }
    anyhow::ensure!(
        offset == bytes.len(),
        "YTsaurus packed refs contain trailing bytes"
    );
    let [first, second] = refs;
    Ok((first, second))
}

pub(super) fn crc64(bytes: &[u8]) -> u64 {
    // Bus checksums cover every streamed attachment. A byte-at-a-time CRC made
    // this function the single-stream throughput ceiling, so consume sixteen
    // bytes per dependency step while preserving YTsaurus' exact polynomial
    // and final byte order.
    let mut crc = 0_u64;
    let mut chunks = bytes.chunks_exact(16);
    for chunk in &mut chunks {
        let first = crc ^ u64::from_le_bytes(chunk[..8].try_into().expect("eight-byte chunk"));
        let second = u64::from_le_bytes(chunk[8..].try_into().expect("eight-byte chunk"));
        crc = CRC64_SLICING_TABLES[15][(first & 0xff) as usize]
            ^ CRC64_SLICING_TABLES[14][((first >> 8) & 0xff) as usize]
            ^ CRC64_SLICING_TABLES[13][((first >> 16) & 0xff) as usize]
            ^ CRC64_SLICING_TABLES[12][((first >> 24) & 0xff) as usize]
            ^ CRC64_SLICING_TABLES[11][((first >> 32) & 0xff) as usize]
            ^ CRC64_SLICING_TABLES[10][((first >> 40) & 0xff) as usize]
            ^ CRC64_SLICING_TABLES[9][((first >> 48) & 0xff) as usize]
            ^ CRC64_SLICING_TABLES[8][((first >> 56) & 0xff) as usize]
            ^ CRC64_SLICING_TABLES[7][(second & 0xff) as usize]
            ^ CRC64_SLICING_TABLES[6][((second >> 8) & 0xff) as usize]
            ^ CRC64_SLICING_TABLES[5][((second >> 16) & 0xff) as usize]
            ^ CRC64_SLICING_TABLES[4][((second >> 24) & 0xff) as usize]
            ^ CRC64_SLICING_TABLES[3][((second >> 32) & 0xff) as usize]
            ^ CRC64_SLICING_TABLES[2][((second >> 40) & 0xff) as usize]
            ^ CRC64_SLICING_TABLES[1][((second >> 48) & 0xff) as usize]
            ^ CRC64_SLICING_TABLES[0][((second >> 56) & 0xff) as usize];
    }
    for byte in chunks.remainder() {
        crc = CRC64_TABLE[usize::from((crc as u8) ^ byte)] ^ (crc >> 8);
    }
    crc.swap_bytes()
}

#[inline]
pub(super) fn checksum_matches(expected: u64, bytes: &[u8]) -> bool {
    // YTsaurus' Bus protocol uses zero as `NullChecksum`: it explicitly means
    // that this part was not checksummed, not that its CRC must equal zero.
    expected == 0 || expected == crc64(bytes)
}
