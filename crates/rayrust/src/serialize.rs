//! Serialization bridge between Rust and Ray C++ SDK.
//!
//! Ray C++ SDK uses msgpack for serialization. We use `rmp-serde` which
//! produces compatible msgpack output.
//!
//! ## 跨语言载荷
//!
//! 带 `XLANG` / `PYTHON` metadata 的载荷前面有 9 字节头：第一个值是 msgpack 段的
//! 长度，其余字节是 Ray 没有清零的填充（`serialization.pxi` 用
//! `make_shared[LocalMemoryBuffer]` 申请缓冲区，内容任意）。长度只占 1、2、3 或
//! 5 字节（正 fixint / `0xcc` / `0xcd` / `0xce`），填充因此是 8、7、6 或 4 字节，
//! 不是固定 8 字节。
//!
//! 这个长度只覆盖 msgpack 段。Python 的 `_serialize_to_msgpack`
//! （`ray/_private/serialization.py`）把 msgpack 表达不了的对象放进 pickle5
//! 段，接在 msgpack 段后面，并把 metadata 改成 `PYTHON`；所以带 pickle5 段的
//! 载荷里声明长度小于「总长减 9」。Ray 的 `MessagePackSerializer` 用
//! `strict_types=True`（`python/ray/includes/serialization.pxi:172-176`），tuple
//! 也整个进 pickle5 段，Python 函数返回元组与返回自定义对象一样被拒。
//!
//! `RAW`（Python 任务返回 `bytes`）没有 9 字节头：`SerializationContext.serialize`
//! 对 `bytes` 返回 `RawSerializedObject`（`ray/_private/serialization.py:713-721`），
//! `NativeObjectStore::GetRaw` 的 `METADATA_STR_RAW` 分支再用
//! `Serializer::Serialize(data, data_size)`（`ray/cpp/include/ray/api/serializer.h:35-40`）
//! 把它包成一个裸 msgpack bin
//! （`cpp/src/ray/runtime/object/native_object_store.cc:119-127`），首字节是
//! `c4` / `c5` / `c6`，按下面的「无头」路径解码，拿到的就是原字节。
//!
//! 判定只能看载荷形状，因为 Ray 2.51.1 的公开 C++ API 拿不到 metadata：
//! `NativeObjectStore::GetRaw` 把 metadata 交给 `CheckException` 之后丢弃，
//! `RayRuntime::Get` 只返回 msgpack 数据。
//!
//! - 声明长度等于剩余字节：msgpack 段就是整段，要求被单个值完整消费，段内残留尾随
//!   字节返回 [`RayError::Serialization`]；
//! - 声明长度小于剩余字节：后面还有 pickle5 段，Rust 无法解码，
//!   `deserialize_xlang` 与 `deserialize_xlang_value` 返回
//!   [`RayError::Serialization`]，需要原始字节的调用方改用
//!   `ObjectRef::get_raw_bytes`；
//! - 声明长度大于剩余字节：同样返回 [`RayError::Serialization`]。
//!
//! 四个公开解码入口（[`deserialize`]、[`deserialize_value`]、[`deserialize_xlang`]、
//! [`deserialize_xlang_value`]）均要求目标 msgpack 字节段被单个值完整消费；首个值
//! 之后残留任何尾随字节都会返回 [`RayError::Serialization`]。
//!
//! 对象读取入口在解码之前先用 `rayrust_sys::error_payload::extract` 判定载荷是否为
//! 未翻译错误对象的帧（`9 + N` 字节的 protobuf）：命中即返回携带该错误文本的
//! [`RayError::Unclassified`]，未命中才解码，解码失败仍是 [`RayError::Serialization`]。
//! 判定必须前置：跨语言路径剥掉 9 字节头后正文是单个完整 msgpack `bin`，对
//! `Vec<u8>` 与 `rmpv::Value` 都能解出成功值；非跨语言路径首值是正文长度、后跟填充
//! 与 `bin` 正文，前置判定让它返回携带错误文本的 [`RayError::Unclassified`]，避免
//! 进入解码器被报告为尾随字节的 [`RayError::Serialization`]。帧规则（总长恰为
//! `9 + N`、正文是 msgpack `bin`、protobuf 顶层字段号与 wire type 都在允许集合内、
//! 文本非空且是合法 UTF-8）之外的字节不会被判定为帧；恰好拼成这些规则的载荷会被报告为
//! [`RayError::Unclassified`]，代价是该载荷不再参与解码。
//!
//! For dynamic/generic deserialization (when the Rust caller doesn't know the
//! exact Python return type), use `rmpv::Value` which is msgpack's equivalent
//! of `serde_json::Value`.

use serde::Serialize;

use crate::error::RayError;

/// XLANG 头长度，与 Ray 的 `kMessagePackOffset` / `XLANG_HEADER_LEN` 一致。
const XLANG_HEADER_LEN: usize = 9;

/// Serialize a value to msgpack bytes.
pub fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>, RayError> {
    let mut buf = Vec::new();
    value.serialize(&mut rmp_serde::Serializer::new(&mut buf))?;
    Ok(buf)
}

/// Deserialize a single value from msgpack bytes.
///
/// The entire `data` slice must be consumed by that single msgpack value; any
/// trailing bytes after the first value return [`RayError::Serialization`].
pub fn deserialize<T: serde::de::DeserializeOwned>(data: &[u8]) -> Result<T, RayError> {
    let mut de = rmp_serde::Deserializer::new(data);
    let value = T::deserialize(&mut de)?;
    let remaining = de.into_inner();
    if !remaining.is_empty() {
        return Err(RayError::Serialization(format!(
            "trailing bytes after msgpack value: {} unconsumed byte(s) out of {}",
            remaining.len(),
            data.len()
        )));
    }
    Ok(value)
}

// ─── Cross-language (XLANG) support ────────────────────────────

/// Build the 9-byte XLANG header for the given payload length.
///
/// Format: [msgpack_int(data_len)] [zero_padding to 9 bytes]
fn build_xlang_header(data_len: usize) -> [u8; XLANG_HEADER_LEN] {
    let mut header = [0u8; XLANG_HEADER_LEN];
    let len = data_len as u64;
    if len <= 0x7f {
        header[0] = len as u8; // positive fixint
    } else if len <= 0xff {
        header[0] = 0xcc; // uint8
        header[1] = len as u8;
    } else if len <= 0xffff {
        header[0] = 0xcd; // uint16
        header[1..3].copy_from_slice(&(len as u16).to_be_bytes());
    } else if len <= 0xffff_ffff {
        header[0] = 0xce; // uint32
        header[1..5].copy_from_slice(&(len as u32).to_be_bytes());
    } else {
        header[0] = 0xcf; // uint64
        header[1..9].copy_from_slice(&len.to_be_bytes());
    }
    header
}

/// Serialize a value and wrap it with the XLANG header.
///
/// Use this when Rust needs to produce data that Python will read via
/// Ray's cross-language deserialization path.
pub fn serialize_xlang<T: Serialize>(value: &T) -> Result<Vec<u8>, RayError> {
    let payload = serialize(value)?;
    let header = build_xlang_header(payload.len());
    let mut buf = Vec::with_capacity(XLANG_HEADER_LEN + payload.len());
    buf.extend_from_slice(&header);
    buf.extend_from_slice(&payload);
    Ok(buf)
}

/// 读出 XLANG 头里声明的 msgpack 段长度。
///
/// 判定沿用 C++ SDK 的 `Serializer::IsXLang`（`ray/api/serializer.h:78-82`）：载荷
/// 至少 9 字节，且开头是一个非负的 msgpack 整数，任何整数编码都算，包括 `0xcf`
/// 的 uint64 与 `0xd3` 的 int64。这里用 `rmpv` 真正解出第一个值来判断类型，不靠
/// 首字节的取值范围。
///
/// Ray 的写入方用 `msgpack.dumps(len(msgpack_data))`，总取最短编码，所以正常载荷
/// 的长度只占 1、2、3 或 5 字节（正 fixint、`0xcc`、`0xcd`、`0xce`），头里剩下的
/// 字节是 Ray 没有清零的填充、内容任意。msgpack 段达到 4 GiB 时长度写成 `0xcf`，
/// 这个编码占满 9 字节、没有填充。[`split_xlang`] 按声明长度切分：正常的 4 GiB
/// 载荷照旧切得对，声明长度超过载荷时被拒。
///
/// 这里只读第一个值。
fn xlang_declared_msgpack_len(data: &[u8]) -> Option<u64> {
    if data.len() < XLANG_HEADER_LEN {
        return None;
    }
    let mut header = &data[..XLANG_HEADER_LEN];
    let value = rmpv::decode::read_value(&mut header).ok()?;
    value.as_u64()
}

/// 按载荷形状切出 msgpack 段。
///
/// `Ok(None)` 表示没有 XLANG 头（本地模式的值、Rust 自己写入的值），调用方
/// 从偏移 0 解码。`Ok(Some(..))` 返回 msgpack 段的切片与尾部字节数；尾部为 0
/// 时 msgpack 段就是整段，大于 0 时后面还有 pickle5 段。`Err` 表示头成立但声明
/// 的长度超过载荷。
fn split_xlang(data: &[u8]) -> Result<Option<(&[u8], usize)>, RayError> {
    let declared = match xlang_declared_msgpack_len(data) {
        Some(declared) => declared,
        None => return Ok(None),
    };
    let body = &data[XLANG_HEADER_LEN..];
    if declared > body.len() as u64 {
        return Err(RayError::Serialization(format!(
            "XLANG header declares {} msgpack bytes, but only {} bytes follow the {} byte header",
            declared,
            body.len(),
            XLANG_HEADER_LEN
        )));
    }
    // 上一步已确认声明长度不超过 body 的长度，所以转成 usize 不会截断。
    let declared = declared as usize;
    Ok(Some((&body[..declared], body.len() - declared)))
}

/// 取出要解码的 msgpack 段；带 pickle5 段的载荷在这里被拒。
fn xlang_msgpack_bytes(data: &[u8]) -> Result<&[u8], RayError> {
    match split_xlang(data)? {
        None => Ok(data),
        Some((msgpack, 0)) => Ok(msgpack),
        Some((msgpack, tail)) => Err(RayError::Serialization(format!(
            "Ray stored the value in a pickle5 section: {} msgpack bytes followed by {} bytes of \
             pickle5. Rust cannot decode pickle5; read the raw bytes with \
             ObjectRef::get_raw_bytes",
            msgpack.len(),
            tail
        ))),
    }
}

/// Deserialize a single value from cross-language (Python) result bytes.
///
/// Payloads carrying `XLANG` / `PYTHON` metadata start with a 9-byte header
/// whose integer field records only the length of the msgpack part. This
/// function strips the header and deserializes the msgpack part.
///
/// `RAW` payloads (a Python task returning `bytes`) arrive as a bare msgpack
/// bin with no header, so they take the no-header path below and decode to the
/// original bytes.
///
/// A payload that also carries a pickle5 section is rejected with
/// [`RayError::Serialization`]: Rust cannot decode pickle5. Read those bytes
/// with `ObjectRef::get_raw_bytes` instead.
///
/// Data without an XLANG header (local mode, values Rust wrote itself) is
/// deserialized from offset 0.
///
/// In both the header and no-header paths, the msgpack byte slice must be
/// consumed in full by a single value; any trailing bytes return
/// [`RayError::Serialization`].
pub fn deserialize_xlang<T: serde::de::DeserializeOwned>(data: &[u8]) -> Result<T, RayError> {
    deserialize(xlang_msgpack_bytes(data)?)
}

/// Deserialize cross-language (Python) result into a generic msgpack Value.
///
/// This is useful when the caller does not know the exact type of the
/// Python return value at compile time. The returned `rmpv::Value` can
/// hold any msgpack type: nil, bool, int, float, str, bin, array, or map.
///
/// A payload that also carries a pickle5 section, or that leaves any trailing
/// bytes unconsumed after the first msgpack value, is rejected with
/// [`RayError::Serialization`]; see [`deserialize_xlang`].
///
/// # Example
/// ```ignore
/// let obj_ref = rayrust::task_call_python("mymod", "complex_func", &args)?;
/// let val: rmpv::Value = obj_ref.get_value()?;
/// match val {
///     rmpv::Value::Array(items) => { /* Python returned a list */ }
///     rmpv::Value::Map(entries) => { /* Python returned a dict */ }
///     _ => {}
/// }
/// ```
pub fn deserialize_xlang_value(data: &[u8]) -> Result<rmpv::Value, RayError> {
    deserialize_value(xlang_msgpack_bytes(data)?)
}

/// Deserialize raw msgpack bytes into a generic msgpack Value (no XLANG header).
///
/// The entire `data` slice must be consumed by a single msgpack value; any
/// trailing bytes after the first value return [`RayError::Serialization`].
pub fn deserialize_value(data: &[u8]) -> Result<rmpv::Value, RayError> {
    let mut reader = data;
    let value = rmpv::decode::read_value(&mut reader)
        .map_err(|e| RayError::Serialization(format!("msgpack value decode error: {}", e)))?;
    if !reader.is_empty() {
        return Err(RayError::Serialization(format!(
            "trailing bytes after msgpack value: {} unconsumed byte(s) out of {}",
            reader.len(),
            data.len()
        )));
    }
    Ok(value)
}

// ─── Tests ─────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use std::collections::HashMap;

    // ── Basic round-trip ──────────────────────────────────────

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        let val: i64 = 42;
        let bytes = serialize(&val).unwrap();
        let back: i64 = deserialize(&bytes).unwrap();
        assert_eq!(val, back);
    }

    // ── XLANG 头：按载荷形状判定与切分 ─────────────────────

    #[test]
    fn test_serialize_xlang_header_format() {
        // 小载荷（长度 <= 0x7f）：首字节就是长度，正 fixint
        let header = build_xlang_header(5);
        assert_eq!(header[0], 5);
        assert_eq!(header[1..], [0u8; 8]);

        // 中等载荷（长度 <= 0xff）：uint8 编码
        let header = build_xlang_header(200);
        assert_eq!(header[0], 0xcc);
        assert_eq!(header[1], 200);
        assert_eq!(header[2..], [0u8; 7]);

        // 更大载荷（长度 <= 0xffff）：uint16 编码
        let header = build_xlang_header(1000);
        assert_eq!(header[0], 0xcd);
        assert_eq!(header[1..3], [0x03, 0xe8]);
    }

    /// 构造一个带 XLANG 头的载荷：声明长度用正 fixint 写，后面跟 `body`。
    fn xlang_payload(declared: usize, body: &[u8]) -> Vec<u8> {
        assert!(
            declared <= 0x7f,
            "these fixtures write the length as a positive fixint"
        );
        let mut data = vec![declared as u8];
        data.extend_from_slice(&[0u8; XLANG_HEADER_LEN - 1]);
        data.extend_from_slice(body);
        data
    }

    #[test]
    fn test_xlang_declared_len_all_encodings() {
        // 覆盖五档 msgpack 无符号整数编码边界（正 fixint、0xcc、0xcd、0xce、0xcf）
        let cases: &[(usize, u8)] = &[
            (0, 0x00),
            (1, 0x01),
            (0x7f, 0x7f),
            (0x80, 0xcc),
            (0xff, 0xcc),
            (0x100, 0xcd),
            (0xffff, 0xcd),
            (0x1_0000, 0xce),
            (0xffff_ffff, 0xce),
            (0x1_0000_0000usize, 0xcf),
        ];
        for &(len, expected_first_byte) in cases {
            let header = build_xlang_header(len);
            assert_eq!(
                header[0], expected_first_byte,
                "first header byte for length {}",
                len
            );
            assert_eq!(
                xlang_declared_msgpack_len(&header),
                Some(len as u64),
                "declared length of a {} byte msgpack part",
                len
            );
        }
    }

    #[test]
    fn test_split_xlang_plain_payload() {
        // 声明长度等于剩余字节：msgpack 段就是整段，没有尾部
        let payload = serialize(&vec![1i64, 2, 3]).unwrap();
        let data = xlang_payload(payload.len(), &payload);
        let (msgpack, tail) = split_xlang(&data).unwrap().expect("an XLANG header");
        assert_eq!(msgpack, payload.as_slice());
        assert_eq!(tail, 0);

        let back: Vec<i64> = deserialize_xlang(&data).unwrap();
        assert_eq!(back, vec![1, 2, 3]);
        let value = deserialize_xlang_value(&data).unwrap();
        assert_eq!(value.as_array().unwrap().len(), 3);
    }

    #[test]
    fn test_split_xlang_pickle5_tail() {
        // Python 侧的形状：msgpack 段只放占位对象，pickle5 段接在后面
        let msgpack = serialize(&0i64).unwrap();
        assert_eq!(msgpack, vec![0x00]);
        let mut data = xlang_payload(msgpack.len(), &msgpack);
        data.extend_from_slice(&[0xaa; 700]);

        let (part, tail) = split_xlang(&data).unwrap().expect("an XLANG header");
        assert_eq!(part, msgpack.as_slice());
        assert_eq!(tail, 700);
    }

    #[test]
    fn test_deserialize_xlang_rejects_pickle5_tail() {
        let msgpack = serialize(&0i64).unwrap();
        let mut data = xlang_payload(msgpack.len(), &msgpack);
        data.extend_from_slice(&[0xaa; 700]);

        match deserialize_xlang::<i64>(&data) {
            Err(RayError::Serialization(msg)) => {
                assert!(msg.contains("pickle5"), "message without pickle5: {}", msg);
                assert!(
                    msg.contains("get_raw_bytes"),
                    "message without the raw-byte hint: {}",
                    msg
                );
                assert!(
                    msg.contains("1 msgpack bytes"),
                    "message without the declared msgpack length: {}",
                    msg
                );
                assert!(
                    msg.contains("700 bytes of pickle5"),
                    "message without the tail size: {}",
                    msg
                );
            }
            other => panic!("a payload with a pickle5 tail returned {:?}", other),
        }

        match deserialize_xlang_value(&data) {
            Err(RayError::Serialization(msg)) => {
                assert!(msg.contains("pickle5"), "message without pickle5: {}", msg)
            }
            other => panic!("a payload with a pickle5 tail returned {:?}", other),
        }
    }

    #[test]
    fn test_deserialize_xlang_rejects_declared_len_beyond_payload() {
        let payload = serialize(&vec![1i64, 2, 3]).unwrap();
        assert_eq!(payload.len(), 4);
        let data = xlang_payload(payload.len() + 5, &payload);

        match deserialize_xlang::<Vec<i64>>(&data) {
            Err(RayError::Serialization(msg)) => {
                assert!(
                    msg.contains("declares 9 msgpack bytes"),
                    "message without the declared length: {}",
                    msg
                );
                assert!(
                    msg.contains("only 4 bytes"),
                    "message without the payload size: {}",
                    msg
                );
            }
            other => panic!("an over-long declared length returned {:?}", other),
        }

        assert!(matches!(
            deserialize_xlang_value(&data),
            Err(RayError::Serialization(_))
        ));
    }

    #[test]
    fn test_deserialize_xlang_without_header_decodes_from_offset_zero() {
        let payload = serialize(&vec![1i64, 2, 3, 4, 5]).unwrap();
        assert!(split_xlang(&payload).unwrap().is_none());

        let back: Vec<i64> = deserialize_xlang(&payload).unwrap();
        assert_eq!(back, vec![1, 2, 3, 4, 5]);
        let value = deserialize_xlang_value(&payload).unwrap();
        assert_eq!(value.as_array().unwrap().len(), 5);
    }

    #[test]
    fn test_short_positive_integer_payload_is_not_xlang() {
        // put(&42i32) 的 1 字节载荷：首字节是正 fixint，但总长不足 9 字节
        let raw = serialize(&42i32).unwrap();
        assert_eq!(raw, vec![42]);
        assert_eq!(xlang_declared_msgpack_len(&raw), None);
        assert!(split_xlang(&raw).unwrap().is_none());

        let back: i64 = deserialize_xlang(&raw).unwrap();
        assert_eq!(back, 42);
    }

    #[test]
    fn test_uint64_len_encoding_is_an_xlang_header() {
        // 9 字节的 uint64 载荷：0xcf 编码的非负整数，与 IsXLang 一样算头；
        // 声明长度 u64::MAX 远超后面的 0 字节，按越界拒绝
        let raw = serialize(&u64::MAX).unwrap();
        assert_eq!(raw.len(), XLANG_HEADER_LEN);
        assert_eq!(raw[0], 0xcf);
        assert_eq!(xlang_declared_msgpack_len(&raw), Some(u64::MAX));
        assert!(matches!(split_xlang(&raw), Err(RayError::Serialization(_))));
        assert!(matches!(
            deserialize_xlang::<u64>(&raw),
            Err(RayError::Serialization(_))
        ));
    }

    #[test]
    fn test_nine_zero_bytes_is_a_header_declaring_no_body() {
        // 恰好 9 字节全零：首值 0 是非负整数，总长够 9 字节，判成头；
        // 声明 0 字节的 msgpack 段，body 为空所以切分成功，但没有可解的值
        let data = [0u8; XLANG_HEADER_LEN];
        assert_eq!(xlang_declared_msgpack_len(&data), Some(0));
        assert_eq!(
            split_xlang(&data).unwrap(),
            Some((&data[XLANG_HEADER_LEN..], 0))
        );
        assert!(matches!(
            deserialize_xlang_value(&data),
            Err(RayError::Serialization(_))
        ));
    }

    #[test]
    fn test_int64_len_encoding_is_an_xlang_header() {
        // 0xd3 编码的 int64 +5 占满 9 字节头，与 IsXLang 一样算头；没有 body，
        // 声明 5 越界
        let mut data = vec![0xd3];
        data.extend_from_slice(&5i64.to_be_bytes());
        assert_eq!(data.len(), XLANG_HEADER_LEN);
        assert_eq!(xlang_declared_msgpack_len(&data), Some(5));
        assert!(matches!(
            split_xlang(&data),
            Err(RayError::Serialization(_))
        ));
        assert!(matches!(
            deserialize_xlang::<i64>(&data),
            Err(RayError::Serialization(_))
        ));

        // 同一个编码写负数则不是非负整数，按没有头处理
        let mut negative = vec![0xd3];
        negative.extend_from_slice(&(-5i64).to_be_bytes());
        assert_eq!(xlang_declared_msgpack_len(&negative), None);
        assert!(split_xlang(&negative).unwrap().is_none());
    }

    #[test]
    fn test_deserialize_xlang_non_zero_padding() {
        // 头部后 8 字节是 Ray 未初始化的填充，可能非零；只解第一个值
        let payload = serialize(&42i64).unwrap(); // 单字节 0x2a
        let mut data = vec![payload.len() as u8];
        data.extend_from_slice(&[0x81, 0x04, 0x64, 0x28, 0x7f, 0x00, 0x00, 0x40]);
        data.extend_from_slice(&payload);
        assert_eq!(data.len(), 10);

        let (msgpack, tail) = split_xlang(&data).unwrap().expect("an XLANG header");
        assert_eq!(msgpack, payload.as_slice());
        assert_eq!(tail, 0);
        let back: i64 = deserialize_xlang(&data).unwrap();
        assert_eq!(back, 42);
    }

    #[test]
    fn test_serialize_xlang_uint8_len_roundtrip() {
        // 载荷 128-255 字节：头部用 uint8 编码（0xcc + 1 字节）
        let payload_str = "x".repeat(200);
        let payload = serialize(&payload_str).unwrap();
        assert!(
            payload.len() > 127 && payload.len() <= 255,
            "payload len = {}, expected 128-255",
            payload.len()
        );
        let xlang = serialize_xlang(&payload_str).unwrap();
        let (msgpack, tail) = split_xlang(&xlang).unwrap().expect("an XLANG header");
        assert_eq!(msgpack.len(), payload.len());
        assert_eq!(tail, 0);
        let back: String = deserialize_xlang(&xlang).unwrap();
        assert_eq!(back.len(), 200);
    }

    #[test]
    fn test_serialize_xlang_uint16_len_roundtrip() {
        // 载荷超过 255 字节：头部用 uint16 编码（0xcd + 2 字节）
        let payload_str = "x".repeat(1000);
        let payload = serialize(&payload_str).unwrap();
        assert!(
            payload.len() > 255,
            "payload len = {}, expected > 255",
            payload.len()
        );
        let xlang = serialize_xlang(&payload_str).unwrap();
        let (msgpack, tail) = split_xlang(&xlang).unwrap().expect("an XLANG header");
        assert_eq!(msgpack.len(), payload.len());
        assert_eq!(tail, 0);
        let back: String = deserialize_xlang(&xlang).unwrap();
        assert_eq!(back.len(), 1000);
    }

    #[test]
    fn test_serialize_xlang_uint32_len_roundtrip() {
        // 载荷超过 65535 字节：头部用 uint32 编码（0xce + 4 字节）
        let big_string = "x".repeat(70000);
        let payload = serialize(&big_string).unwrap();
        assert!(payload.len() > 65535);
        let xlang = serialize_xlang(&big_string).unwrap();
        let (msgpack, tail) = split_xlang(&xlang).unwrap().expect("an XLANG header");
        assert_eq!(msgpack.len(), payload.len());
        assert_eq!(tail, 0);
        let back: String = deserialize_xlang(&xlang).unwrap();
        assert_eq!(back.len(), 70000);
    }

    // ── XLANG round-trip: all primitive types ──────────────────

    #[test]
    fn test_serialize_xlang_roundtrip() {
        let val = vec![1i64, 2, 3, 4, 5];
        let xlang_bytes = serialize_xlang(&val).unwrap();
        let payload_len = serialize(&val).unwrap().len();
        assert_eq!(xlang_bytes.len(), XLANG_HEADER_LEN + payload_len);
        let back: Vec<i64> = deserialize_xlang(&xlang_bytes).unwrap();
        assert_eq!(val, back);
    }

    #[test]
    fn test_serialize_xlang_string() {
        let val = "hello".to_string();
        let xlang_bytes = serialize_xlang(&val).unwrap();
        let back: String = deserialize_xlang(&xlang_bytes).unwrap();
        assert_eq!(val, back);
    }

    #[test]
    fn test_serialize_xlang_bool() {
        let xlang = serialize_xlang(&true).unwrap();
        let back: bool = deserialize_xlang(&xlang).unwrap();
        assert!(back);

        let xlang = serialize_xlang(&false).unwrap();
        let back: bool = deserialize_xlang(&xlang).unwrap();
        assert!(!back);
    }

    #[test]
    fn test_serialize_xlang_f64() {
        let val: f64 = std::f64::consts::PI;
        let xlang = serialize_xlang(&val).unwrap();
        let back: f64 = deserialize_xlang(&xlang).unwrap();
        assert_eq!(val, back);
    }

    #[test]
    fn test_serialize_xlang_none() {
        let val: Option<i64> = None;
        let xlang = serialize_xlang(&val).unwrap();
        let back: Option<i64> = deserialize_xlang(&xlang).unwrap();
        assert_eq!(val, back);
    }

    #[test]
    fn test_serialize_xlang_i32() {
        let val: i32 = -12345;
        let xlang = serialize_xlang(&val).unwrap();
        let back: i32 = deserialize_xlang(&xlang).unwrap();
        assert_eq!(val, back);
    }

    // ── XLANG round-trip: complex types ────────────────────────

    #[test]
    fn test_serialize_xlang_empty_list() {
        let val: Vec<i64> = vec![];
        let xlang = serialize_xlang(&val).unwrap();
        let back: Vec<i64> = deserialize_xlang(&xlang).unwrap();
        assert_eq!(val, back);
        assert!(back.is_empty());
    }

    #[test]
    fn test_serialize_xlang_empty_map() {
        let val: HashMap<String, i64> = HashMap::new();
        let xlang = serialize_xlang(&val).unwrap();
        let back: HashMap<String, i64> = deserialize_xlang(&xlang).unwrap();
        assert_eq!(val, back);
        assert!(back.is_empty());
    }

    #[test]
    fn test_serialize_xlang_nested_vec() {
        let val: Vec<Vec<i64>> = vec![vec![1, 2], vec![3, 4, 5], vec![]];
        let xlang = serialize_xlang(&val).unwrap();
        let back: Vec<Vec<i64>> = deserialize_xlang(&xlang).unwrap();
        assert_eq!(val, back);
    }

    #[test]
    fn test_serialize_xlang_tuple() {
        let val = (42i64, "hello".to_string(), true);
        let xlang = serialize_xlang(&val).unwrap();
        let back: (i64, String, bool) = deserialize_xlang(&xlang).unwrap();
        assert_eq!(val, back);
    }

    #[test]
    fn test_serialize_xlang_large_list() {
        let val: Vec<i64> = (0..10000).collect();
        let xlang = serialize_xlang(&val).unwrap();
        let back: Vec<i64> = deserialize_xlang(&xlang).unwrap();
        assert_eq!(val, back);
    }

    // ── Auto-detect: with and without header ──────────────────

    #[test]
    fn test_deserialize_xlang_auto_detect_no_header() {
        let val = vec![1i64, 2, 3, 4, 5];
        let raw_bytes = serialize(&val).unwrap();
        let back: Vec<i64> = deserialize_xlang(&raw_bytes).unwrap();
        assert_eq!(val, back);
    }

    #[test]
    fn test_deserialize_xlang_auto_detect_with_header() {
        let val = vec![1i64, 2, 3, 4, 5];
        let xlang_bytes = serialize_xlang(&val).unwrap();
        let back: Vec<i64> = deserialize_xlang(&xlang_bytes).unwrap();
        assert_eq!(val, back);
    }

    #[test]
    fn test_deserialize_xlang_auto_detect_non_zero_padding() {
        // Simulate Ray 2.51.1+ where header has metadata in padding
        let payload = serialize(&vec![1i64, 2, 3]).unwrap();
        let mut data = vec![payload.len() as u8];
        data.extend_from_slice(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
        data.extend_from_slice(&payload);
        let back: Vec<i64> = deserialize_xlang(&data).unwrap();
        assert_eq!(back, vec![1, 2, 3]);
    }

    // ── rmpv::Value deserialization ────────────────────────────

    #[test]
    fn test_deserialize_xlang_value_list() {
        let val = vec![10i64, 20, 30];
        let xlang_bytes = serialize_xlang(&val).unwrap();
        let value = deserialize_xlang_value(&xlang_bytes).unwrap();
        assert!(value.is_array());
        let arr = value.as_array().unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0].as_i64().unwrap(), 10);
        assert_eq!(arr[1].as_i64().unwrap(), 20);
        assert_eq!(arr[2].as_i64().unwrap(), 30);
    }

    #[test]
    fn test_deserialize_xlang_value_dict() {
        let map_value = rmpv::Value::Map(vec![
            (
                rmpv::Value::String("name".into()),
                rmpv::Value::String("alice".into()),
            ),
            (
                rmpv::Value::String("age".into()),
                rmpv::Value::Integer(30.into()),
            ),
        ]);
        let xlang_bytes = serialize_xlang(&map_value).unwrap();
        let value = deserialize_xlang_value(&xlang_bytes).unwrap();
        assert!(value.is_map());

        let entries = value.as_map().unwrap();
        let name_val = entries
            .iter()
            .find(|(k, _)| k.as_str() == Some("name"))
            .map(|(_, v)| v)
            .unwrap();
        assert_eq!(name_val.as_str().unwrap(), "alice");

        let age_val = entries
            .iter()
            .find(|(k, _)| k.as_str() == Some("age"))
            .map(|(_, v)| v)
            .unwrap();
        assert_eq!(age_val.as_i64().unwrap(), 30);
    }

    #[test]
    fn test_deserialize_xlang_value_none() {
        let val: Option<i64> = None;
        let xlang_bytes = serialize_xlang(&val).unwrap();
        let value = deserialize_xlang_value(&xlang_bytes).unwrap();
        assert!(value.is_nil());
    }

    #[test]
    fn test_deserialize_xlang_value_nested() {
        let mut inner1 = HashMap::new();
        inner1.insert("id".to_string(), 1i64);
        let mut inner2 = HashMap::new();
        inner2.insert("id".to_string(), 2i64);
        let mut outer = HashMap::new();
        outer.insert("items".to_string(), vec![inner1, inner2]);

        let xlang_bytes = serialize_xlang(&outer).unwrap();
        let value = deserialize_xlang_value(&xlang_bytes).unwrap();
        assert!(value.is_map());

        let entries = value.as_map().unwrap();
        let items = entries
            .iter()
            .find(|(k, _)| k.as_str() == Some("items"))
            .map(|(_, v)| v)
            .unwrap();
        assert!(items.is_array());
        assert_eq!(items.as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_deserialize_xlang_value_mixed_array() {
        // Simulate Python's [42, "hello", True, None, 3.14]
        let mixed = rmpv::Value::Array(vec![
            rmpv::Value::Integer(42.into()),
            rmpv::Value::String("hello".into()),
            rmpv::Value::Boolean(true),
            rmpv::Value::Nil,
            rmpv::Value::F32(3.15),
        ]);
        let xlang = serialize_xlang(&mixed).unwrap();
        let val = deserialize_xlang_value(&xlang).unwrap();
        assert!(val.is_array());
        let arr = val.as_array().unwrap();
        assert_eq!(arr.len(), 5);
        assert_eq!(arr[0].as_i64().unwrap(), 42);
        assert_eq!(arr[1].as_str().unwrap(), "hello");
        assert!(arr[2].as_bool().unwrap());
        assert!(arr[3].is_nil());
    }

    #[test]
    fn test_deserialize_xlang_value_empty_list() {
        let val: Vec<i64> = vec![];
        let xlang = serialize_xlang(&val).unwrap();
        let value = deserialize_xlang_value(&xlang).unwrap();
        assert!(value.is_array());
        assert_eq!(value.as_array().unwrap().len(), 0);
    }

    #[test]
    fn test_deserialize_value_string() {
        let val = "hello world".to_string();
        let bytes = serialize(&val).unwrap();
        let value = deserialize_value(&bytes).unwrap();
        assert_eq!(value.as_str().unwrap(), "hello world");
    }

    #[test]
    fn test_deserialize_value_bool() {
        let bytes = serialize(&true).unwrap();
        let value = deserialize_value(&bytes).unwrap();
        assert!(value.as_bool().unwrap());
    }

    // ── 误判防范：看起来像 XLANG 的普通 msgpack ─────────────────

    #[test]
    fn test_split_xlang_false_positive_guard() {
        // 1 字节的 fixint 0：总长不足 9 字节
        let raw = serialize(&0i64).unwrap();
        assert!(split_xlang(&raw).unwrap().is_none());

        // fixarray 9 个元素：首字节 0x99 是数组标记，不是整数
        let raw = serialize(&vec![1i64, 2, 3, 4, 5, 6, 7, 8, 9]).unwrap();
        assert_eq!(raw.len(), 10);
        assert_eq!(raw[0], 0x99);
        assert!(split_xlang(&raw).unwrap().is_none());
        let back: Vec<i64> = deserialize_xlang(&raw).unwrap();
        assert_eq!(back, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);

        // 空载荷与短载荷
        assert!(split_xlang(&[]).unwrap().is_none());
        assert!(split_xlang(&[0u8; 5]).unwrap().is_none());
    }

    #[test]
    fn test_serialize_xlang_then_serialize_again() {
        // 重复包一层头时，外层头声明的长度覆盖内层整段的 msgpack 编码，
        // 两次剥离各自正确
        let val = 42i64;
        let xlang1 = serialize_xlang(&val).unwrap();
        let xlang2 = serialize_xlang(&xlang1).unwrap();

        let (outer, tail) = split_xlang(&xlang2).unwrap().expect("an XLANG header");
        assert_eq!(tail, 0);
        let inner_from_outer: Vec<u8> = deserialize(outer).unwrap();
        assert_eq!(inner_from_outer, xlang1);

        let inner: Vec<u8> = deserialize_xlang(&xlang2).unwrap();
        assert_eq!(inner, xlang1);

        let (inner_part, inner_tail) = split_xlang(&inner).unwrap().expect("an XLANG header");
        assert_eq!(inner_tail, 0);
        assert_eq!(inner_part, &inner[XLANG_HEADER_LEN..]);

        let val_back: i64 = deserialize_xlang(&inner).unwrap();
        assert_eq!(val_back, 42);
    }

    // ── 尾随字节拒绝：四个公开解码入口均要求单值完整消费 ──────────

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct SampleRecord {
        id: i64,
        label: String,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    enum SampleEnum {
        Unit,
        Newtype(i64),
        Tuple(i64, String),
        Struct { ok: bool },
    }

    fn assert_trailing_error<T: std::fmt::Debug>(
        res: Result<T, RayError>,
        expected_unconsumed: usize,
        expected_total: usize,
        context: &str,
    ) {
        let expected_msg = format!(
            "trailing bytes after msgpack value: {} unconsumed byte(s) out of {}",
            expected_unconsumed, expected_total
        );
        match res {
            Err(RayError::Serialization(msg)) => {
                assert_eq!(msg, expected_msg, "unexpected message for {context}");
            }
            other => panic!("expected Err(RayError::Serialization({expected_msg:?})) for {context}, got {other:?}"),
        }
    }

    fn check_typed_trailing<T>(val: &T, label: &str)
    where
        T: Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        let encoded = serialize(val).unwrap();
        let clean: T = deserialize(&encoded).unwrap();
        assert_eq!(&clean, val, "{label} clean round-trip");

        // 有效编码 + 1 个尾随字节（覆盖正 fixint 0x00、nil 0xc0、保留标记 0xc1、fixarray(1) 0x91）
        for &tail_byte in &[0x00u8, 0xc0, 0xc1, 0x91] {
            let mut with_one = encoded.clone();
            with_one.push(tail_byte);
            assert_trailing_error(
                deserialize::<T>(&with_one),
                1,
                with_one.len(),
                &format!("{label} + {tail_byte:#04x}"),
            );
        }

        // 有效编码 + 第二个完整编码值
        let extra = serialize(&("extra".to_string(), 99i64)).unwrap();
        let mut with_second = encoded.clone();
        with_second.extend_from_slice(&extra);
        assert_trailing_error(
            deserialize::<T>(&with_second),
            extra.len(),
            with_second.len(),
            &format!("{label} + second value"),
        );
    }

    #[test]
    fn test_deserialize_rejects_trailing_bytes_across_types() {
        check_typed_trailing(&(), "unit");
        check_typed_trailing(&None::<i64>, "Option::None");
        check_typed_trailing(&Some(7i64), "Option::Some(7)");
        check_typed_trailing(&true, "bool(true)");
        check_typed_trailing(&false, "bool(false)");
        check_typed_trailing(&(-42i64), "i64(-42)");
        check_typed_trailing(&42i64, "i64(42)");
        check_typed_trailing(&"hello".to_string(), "String");
        check_typed_trailing(&vec![1i64, 2, 3], "Vec<i64>");
        check_typed_trailing(
            &SampleRecord {
                id: 11,
                label: "item".to_string(),
            },
            "SampleRecord",
        );
        check_typed_trailing(&(9i64, "tuple".to_string(), true), "tuple");

        // 覆盖枚举单元变体与带数据变体，确认 Deserializer 的 marker 缓存路径不会吞掉 1 个尾随字节
        check_typed_trailing(&SampleEnum::Unit, "SampleEnum::Unit");
        check_typed_trailing(&SampleEnum::Newtype(99), "SampleEnum::Newtype");
        check_typed_trailing(
            &SampleEnum::Tuple(5, "tag".to_string()),
            "SampleEnum::Tuple",
        );
        check_typed_trailing(&SampleEnum::Struct { ok: true }, "SampleEnum::Struct");
    }

    #[test]
    fn test_local_mode_pack_error_rejected_by_all_decoders() {
        // 本地模式 PackError 形状：0xc0 (nil) 后跟 (3i32, "boom".to_string()) 的 msgpack 编码
        let mut pack_error = serialize(&()).unwrap();
        assert_eq!(pack_error, vec![0xc0]);
        let err_tuple = serialize(&(3i32, "boom".to_string())).unwrap();
        assert_eq!(&err_tuple[..2], &[0x92, 0x03]);
        pack_error.extend_from_slice(&err_tuple);
        assert_eq!(&pack_error[..3], &[0xc0, 0x92, 0x03]);
        assert!(split_xlang(&pack_error).unwrap().is_none());

        let unconsumed = err_tuple.len();
        let total = pack_error.len();

        assert_trailing_error(
            deserialize::<()>(&pack_error),
            unconsumed,
            total,
            "PackError as ()",
        );
        assert_trailing_error(
            deserialize::<Option<i64>>(&pack_error),
            unconsumed,
            total,
            "PackError as Option<i64>",
        );
        assert_trailing_error(
            deserialize_value(&pack_error),
            unconsumed,
            total,
            "PackError as rmpv::Value",
        );
        assert_trailing_error(
            deserialize_xlang::<()>(&pack_error),
            unconsumed,
            total,
            "PackError via deserialize_xlang::<()>",
        );
        assert_trailing_error(
            deserialize_xlang::<Option<i64>>(&pack_error),
            unconsumed,
            total,
            "PackError via deserialize_xlang::<Option<i64>>",
        );
        assert_trailing_error(
            deserialize_xlang_value(&pack_error),
            unconsumed,
            total,
            "PackError via deserialize_xlang_value",
        );
    }

    #[test]
    fn test_deserialize_value_rejects_trailing_bytes() {
        let fixtures: Vec<(&str, Vec<u8>)> = vec![
            ("Nil", serialize(&()).unwrap()),
            ("Integer", serialize(&42i64).unwrap()),
            ("String", serialize(&"abc".to_string()).unwrap()),
            ("Array", serialize(&vec![1i64, 2, 3]).unwrap()),
            (
                "Map",
                serialize(&rmpv::Value::Map(vec![(
                    rmpv::Value::String("k".into()),
                    rmpv::Value::Integer(1.into()),
                )]))
                .unwrap(),
            ),
        ];
        let second_val = serialize(&vec!["trailing".to_string()]).unwrap();

        for (name, encoded) in fixtures {
            assert!(deserialize_value(&encoded).is_ok(), "{name} clean decode");

            for &tail_byte in &[0x00u8, 0xc0, 0xc1, 0x91] {
                let mut with_one = encoded.clone();
                with_one.push(tail_byte);
                assert_trailing_error(
                    deserialize_value(&with_one),
                    1,
                    with_one.len(),
                    &format!("deserialize_value({name}) + {tail_byte:#04x}"),
                );
            }

            let mut with_second = encoded.clone();
            with_second.extend_from_slice(&second_val);
            assert_trailing_error(
                deserialize_value(&with_second),
                second_val.len(),
                with_second.len(),
                &format!("deserialize_value({name}) + second value"),
            );
        }
    }

    #[test]
    fn test_deserialize_xlang_rejects_trailing_bytes_inside_msgpack_and_fallback() {
        let payload = serialize(&vec![10i64, 20, 30]).unwrap();
        let second = serialize(&"extra-value".to_string()).unwrap();

        // 1. 有 XLANG 头且 tail == 0，声明的 msgpack 段内部在首个值之后跟 1 字节
        for &tail_byte in &[0x00u8, 0xc0, 0xc1, 0x91] {
            let mut body = payload.clone();
            body.push(tail_byte);
            let data = xlang_payload(body.len(), &body);
            let (part, tail) = split_xlang(&data).unwrap().expect("an XLANG header");
            assert_eq!(part.len(), body.len());
            assert_eq!(tail, 0);

            assert_trailing_error(
                deserialize_xlang::<Vec<i64>>(&data),
                1,
                body.len(),
                &format!("xlang msgpack section + {tail_byte:#04x}"),
            );
            assert_trailing_error(
                deserialize_xlang_value(&data),
                1,
                body.len(),
                &format!("xlang_value msgpack section + {tail_byte:#04x}"),
            );
        }

        // 2. 有 XLANG 头且 tail == 0，声明的 msgpack 段内部跟第二个完整值
        let mut body_two = payload.clone();
        body_two.extend_from_slice(&second);
        let data_two = xlang_payload(body_two.len(), &body_two);
        assert_trailing_error(
            deserialize_xlang::<Vec<i64>>(&data_two),
            second.len(),
            body_two.len(),
            "xlang msgpack section + second value",
        );
        assert_trailing_error(
            deserialize_xlang_value(&data_two),
            second.len(),
            body_two.len(),
            "xlang_value msgpack section + second value",
        );

        // 3. 无 XLANG 头的回退路径（短正整数载荷与非整数开头的长载荷）带有尾随字节
        let mut short_fallback = serialize(&42i32).unwrap();
        short_fallback.push(0x00);
        assert!(short_fallback.len() < XLANG_HEADER_LEN);
        assert!(split_xlang(&short_fallback).unwrap().is_none());
        assert_trailing_error(
            deserialize_xlang::<i32>(&short_fallback),
            1,
            short_fallback.len(),
            "short fallback + 0x00",
        );
        assert_trailing_error(
            deserialize_xlang_value(&short_fallback),
            1,
            short_fallback.len(),
            "short fallback value + 0x00",
        );

        let mut long_fallback = serialize(&"non-integer-prefix-payload".to_string()).unwrap();
        long_fallback.extend_from_slice(&second);
        assert!(long_fallback.len() >= XLANG_HEADER_LEN);
        assert!(split_xlang(&long_fallback).unwrap().is_none());
        assert_trailing_error(
            deserialize_xlang::<String>(&long_fallback),
            second.len(),
            long_fallback.len(),
            "long fallback + second value",
        );
        assert_trailing_error(
            deserialize_xlang_value(&long_fallback),
            second.len(),
            long_fallback.len(),
            "long fallback value + second value",
        );

        // 4. 对照：tail > 0（pickle5 段）仍返回原有的 pickle5 错误消息
        let mut with_pickle5 = xlang_payload(payload.len(), &payload);
        with_pickle5.extend_from_slice(&[0x80, 0x05, 0x95]);
        match deserialize_xlang::<Vec<i64>>(&with_pickle5) {
            Err(RayError::Serialization(msg)) => {
                assert!(msg.contains("pickle5"), "expected pickle5 message: {msg}");
            }
            other => panic!("expected pickle5 Serialization error, got {other:?}"),
        }
        match deserialize_xlang_value(&with_pickle5) {
            Err(RayError::Serialization(msg)) => {
                assert!(msg.contains("pickle5"), "expected pickle5 message: {msg}");
            }
            other => panic!("expected pickle5 Serialization error, got {other:?}"),
        }
    }
}
