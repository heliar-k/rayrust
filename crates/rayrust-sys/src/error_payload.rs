//! 解码 Ray 失败对象的二进制错误载荷。
//!
//! 跨语言任务失败与系统级错误对象经 `MakeSerializedErrorBuffer`
//! （`src/ray/common/ray_object.cc:56-91`）或跨语言执行器
//! （`cpp/src/ray/runtime/task/task_executor.cc:226-233`、`:264-273`）封装成：
//! `[msgpack 整数 N + 填充字节][N 字节 msgpack bin（内含 protobuf）]`。
//! 其中：
//! - `rpc.RayException`（`src/ray/protobuf/common.proto:318-325`）在字段 3
//!   （`formatted_exception_string`）存放格式化异常文本；
//! - `rpc.RayErrorInfo`（`src/ray/protobuf/common.proto:273-286`）在字段 5
//!   （`error_message`）存放错误原因文本。
//!
//! 解析成功且文本为非空合法 UTF-8 时返回去除尾部空白后的文本；其余情况一律按
//! `String::from_utf8_lossy` 原样呈现全部字节。

/// 将错误通道记录的载荷字节渲染为可读字符串。
///
/// 若载荷符合 Ray 序列化错误缓冲格式且能提取出非空 UTF-8 格式化文本，返回去除尾部
/// 空白后的该文本；否则按 `String::from_utf8_lossy` 转换全部原始字节。
pub fn render(payload: &[u8]) -> String {
    extract_formatted_text(payload).unwrap_or_else(|| String::from_utf8_lossy(payload).into_owned())
}

/// `MakeSerializedErrorBuffer` 给正文预留的固定头部长度
/// （`src/ray/common/constants.h:61`、`src/ray/common/ray_object.cc:79-90`）：msgpack
/// 整数写在偏移 0，正文从偏移 9 开始，缓冲区总长恒为 `9 + 正文长度`。正文位于载荷
/// 末尾与头部长度恒为 9 这两条一起把「任意可打印文本恰好形如帧」的误判挡住。
const K_MESSAGEPACK_OFFSET: usize = 9;

fn extract_formatted_text(payload: &[u8]) -> Option<String> {
    let (body_len, int_len) = read_msgpack_uint(payload)?;
    if body_len == 0 || int_len > K_MESSAGEPACK_OFFSET {
        return None;
    }
    if payload.len() != K_MESSAGEPACK_OFFSET.checked_add(body_len)? {
        return None;
    }
    let body = &payload[K_MESSAGEPACK_OFFSET..];
    let pb = read_msgpack_bin(body)?;
    scan_protobuf_error_text(pb).map(|text| text.to_string())
}

/// 读取切片开头的无符号 msgpack 整数，返回 `(数值, 整数编码占用的字节数)`。
fn read_msgpack_uint(data: &[u8]) -> Option<(usize, usize)> {
    let first = *data.first()?;
    match first {
        0x00..=0x7f => Some((first as usize, 1)),
        0xcc => {
            let v = *data.get(1)?;
            Some((v as usize, 2))
        }
        0xcd => {
            let bytes: [u8; 2] = data.get(1..3)?.try_into().ok()?;
            Some((u16::from_be_bytes(bytes) as usize, 3))
        }
        0xce => {
            let bytes: [u8; 4] = data.get(1..5)?.try_into().ok()?;
            let val = usize::try_from(u32::from_be_bytes(bytes)).ok()?;
            Some((val, 5))
        }
        0xcf => {
            let bytes: [u8; 8] = data.get(1..9)?.try_into().ok()?;
            let val = usize::try_from(u64::from_be_bytes(bytes)).ok()?;
            Some((val, 9))
        }
        _ => None,
    }
}

/// 解析占满整个 `body` 的 msgpack `bin8` / `bin16` / `bin32` 并返回其内容切片。
fn read_msgpack_bin(body: &[u8]) -> Option<&[u8]> {
    let first = *body.first()?;
    let (len, header_len) = match first {
        0xc4 => (*body.get(1)? as usize, 2usize),
        0xc5 => {
            let bytes: [u8; 2] = body.get(1..3)?.try_into().ok()?;
            (u16::from_be_bytes(bytes) as usize, 3usize)
        }
        0xc6 => {
            let bytes: [u8; 4] = body.get(1..5)?.try_into().ok()?;
            let val = usize::try_from(u32::from_be_bytes(bytes)).ok()?;
            (val, 5usize)
        }
        _ => return None,
    };
    if header_len.checked_add(len)? != body.len() {
        return None;
    }
    Some(&body[header_len..])
}

/// 读取 protobuf base-128 varint（最多 10 字节）。
fn read_varint(data: &[u8], pos: &mut usize) -> Option<u64> {
    let mut value = 0u64;
    let mut shift = 0u32;
    for i in 0..10 {
        let byte = *data.get(*pos)?;
        *pos += 1;
        if i == 9 && byte > 0x01 {
            return None;
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some(value);
        }
        shift += 7;
    }
    None
}

/// `RayException` 与 `RayErrorInfo` 顶层字段的 wire type 表（`common.proto:273-286`、
/// `:318-325`）。两消息都不认识的字段号说明这段字节不属于错误载荷（例如恰好形如帧的
/// 可打印文本），整段按未知形状回退。
fn expected_wire_type(field_number: u64) -> Option<u8> {
    match field_number {
        1 => Some(0),  // RayException.language
        2 => Some(2),  // RayException.serialized_exception / RayErrorInfo.actor_died_error
        3 => Some(2), // RayException.formatted_exception_string / RayErrorInfo.runtime_env_setup_failed_error
        4 => Some(2), // RayErrorInfo.actor_unavailable_error
        5 => Some(2), // RayErrorInfo.error_message
        11 => Some(0), // RayErrorInfo.error_type
        _ => None,
    }
}

/// 扫描 protobuf 顶层字段并选出可读文本。
///
/// 两类消息共用这条信封：`RayException`（字段 1、字段 2、字段 3
/// `formatted_exception_string`）与 `RayErrorInfo`（字段 2、字段 3
/// `runtime_env_setup_failed_error` 嵌套消息、字段 4、字段 5 `error_message`、字段 11
/// `error_type`，见 `src/ray/protobuf/common.proto:273-286` 与 `:318-325`）。字段 5
/// 只出现在 `RayErrorInfo` 里，`RayException` 不会携带；`RayErrorInfo` 的字段 3 是嵌套
/// 消息，把它的线格式字节当成文本会输出 `\n`、`ESC` 之类的头部字节。所以：字段 5 存在时
/// 取它；否则只有字段 4/11 也未出现（说明这不是 `RayErrorInfo`）时才取字段 3。
///
/// 任何截断或未知 wire type 视为整段解析失败。
fn scan_protobuf_error_text(pb: &[u8]) -> Option<&str> {
    let mut pos = 0usize;
    let mut field3: Option<&[u8]> = None;
    let mut field5: Option<&[u8]> = None;
    let mut error_info_only_field = false;

    while pos < pb.len() {
        let tag = read_varint(pb, &mut pos)?;
        let field_number = tag >> 3;
        let wire_type = (tag & 0x07) as u8;
        if field_number == 0 {
            return None;
        }
        if expected_wire_type(field_number)? != wire_type {
            return None;
        }
        if field_number == 4 || field_number == 11 {
            error_info_only_field = true;
        }
        match wire_type {
            0 => {
                read_varint(pb, &mut pos)?;
            }
            1 => {
                pos = pos.checked_add(8)?;
                if pos > pb.len() {
                    return None;
                }
            }
            2 => {
                let len = usize::try_from(read_varint(pb, &mut pos)?).ok()?;
                let end = pos.checked_add(len)?;
                let slice = pb.get(pos..end)?;
                pos = end;
                if field_number == 3 {
                    field3 = Some(slice);
                } else if field_number == 5 {
                    field5 = Some(slice);
                }
            }
            5 => {
                pos = pos.checked_add(4)?;
                if pos > pb.len() {
                    return None;
                }
            }
            _ => return None,
        }
    }

    let candidate = match field5 {
        Some(text) => Some(text),
        None if !error_info_only_field => field3,
        None => None,
    };
    candidate.and_then(printable_text)
}

/// 候选字节切片按文本校验：合法 UTF-8、去除尾部空白后仍非空。
fn printable_text(raw: &[u8]) -> Option<&str> {
    let text = std::str::from_utf8(raw).ok()?.trim_end();
    if text.is_empty() {
        None
    } else {
        Some(text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn push_varint(buf: &mut Vec<u8>, mut val: u64) {
        loop {
            let mut byte = (val & 0x7f) as u8;
            val >>= 7;
            if val != 0 {
                byte |= 0x80;
            }
            buf.push(byte);
            if val == 0 {
                break;
            }
        }
    }

    fn push_len_delimited(buf: &mut Vec<u8>, field_number: u64, data: &[u8]) {
        push_varint(buf, (field_number << 3) | 2);
        push_varint(buf, data.len() as u64);
        buf.extend_from_slice(data);
    }

    fn wrap_bin16_with_9byte_header(pb: &[u8], padding: [u8; 6]) -> Vec<u8> {
        assert!(pb.len() <= u16::MAX as usize);
        let mut body = Vec::with_capacity(3 + pb.len());
        body.push(0xc5);
        body.extend_from_slice(&(pb.len() as u16).to_be_bytes());
        body.extend_from_slice(pb);

        let mut payload = Vec::with_capacity(9 + body.len());
        payload.push(0xcd);
        payload.extend_from_slice(&(body.len() as u16).to_be_bytes());
        payload.extend_from_slice(&padding);
        payload.extend_from_slice(&body);
        payload
    }

    #[test]
    fn renders_ray_exception_field3_from_measured_bin16_shape() {
        let mut pb = Vec::new();
        // 字段 1：language = PYTHON (0)，显式编码 varint
        push_varint(&mut pb, (1 << 3) | 0);
        push_varint(&mut pb, 0);
        // 字段 2：serialized_exception（模拟 517 字节含 NUL 与高位字节的 pickle）
        let mut fake_pickle = vec![0x80, 0x05, 0x95, 0xfa, 0x01, 0x00, 0xff];
        fake_pickle.resize(517, 0xa3);
        push_len_delimited(&mut pb, 2, &fake_pickle);
        // 字段 3：formatted_exception_string（带尾部换行）
        let traceback = concat!(
            "Traceback (most recent call last):\n",
            "  File \"rayrust_test.py\", line 42, in fail\n",
            "ValueError: python-task-boom-t37\n",
        );
        push_len_delimited(&mut pb, 3, traceback.as_bytes());

        let payload = wrap_bin16_with_9byte_header(&pb, [0x00, 0x7f, 0x00, 0x11, 0x00, 0xfe]);
        let rendered = render(&payload);
        assert_eq!(
            rendered,
            traceback.trim_end(),
            "字段 3 的格式化异常文本应当原样取出并去除尾部空白"
        );
        assert!(!rendered.contains('\u{FFFD}'));
    }

    #[test]
    fn renders_bin8_body_with_fixint_and_uint8_headers() {
        let mut pb = Vec::new();
        let msg = "Traceback (most recent call last):\nRuntimeError: short-boom\n  ";
        push_len_delimited(&mut pb, 3, msg.as_bytes());
        assert!(pb.len() < 120);

        // bin8 正文 + 正 fixint 长度 + 8 字节填充（共 9 字节头）
        let mut body = vec![0xc4, pb.len() as u8];
        body.extend_from_slice(&pb);
        let mut payload_fixint = vec![body.len() as u8];
        payload_fixint.extend_from_slice(&[0x00, 0xde, 0xad, 0xbe, 0xef, 0x00, 0x01, 0x02]);
        payload_fixint.extend_from_slice(&body);
        assert_eq!(render(&payload_fixint), msg.trim_end());

        // bin8 正文 + cc uint8 长度（模拟 140..250 字节正文）
        let mut medium_pb = Vec::new();
        push_len_delimited(&mut medium_pb, 2, &[0xff; 150]);
        push_len_delimited(&mut medium_pb, 3, b"ValueError: bin8-uint8\n");
        let mut medium_body = vec![0xc4, medium_pb.len() as u8];
        medium_body.extend_from_slice(&medium_pb);
        let mut payload_u8 = vec![0xcc, medium_body.len() as u8];
        payload_u8.extend_from_slice(&[0u8; 7]);
        payload_u8.extend_from_slice(&medium_body);
        assert_eq!(render(&payload_u8), "ValueError: bin8-uint8");
    }

    #[test]
    fn renders_ray_error_info_field5_when_field3_absent() {
        let mut pb = Vec::new();
        // 字段 2：嵌套的 ActorDeathCause 字节（含非 UTF-8 字节）
        push_len_delimited(&mut pb, 2, &[0x0a, 0x04, 0xff, 0x00, 0xfe, 0x01]);
        // 字段 5：RayErrorInfo.error_message
        let err_msg = "The actor died unexpectedly before finishing this task.\n";
        push_len_delimited(&mut pb, 5, err_msg.as_bytes());
        // 字段 11：error_type = ACTOR_DIED (varint)
        push_varint(&mut pb, (11 << 3) | 0);
        push_varint(&mut pb, 3);

        let payload = wrap_bin16_with_9byte_header(&pb, [0u8; 6]);
        assert_eq!(render(&payload), err_msg.trim_end());
    }

    #[test]
    fn preserves_plain_text_payload_verbatim() {
        let cases: &[&[u8]] = &[
            concat!(
                "Invalid: An exception was thrown while executing function(",
                "error_kinds_test_unregistered_function): RayFunctionNotFound: ",
                "Executable function not found\n"
            )
            .as_bytes(),
            b"Get object error: TimedOut: Get timed out: some object(s) not ready.",
            b"short text with \xc5\x00 inside",
        ];
        for &plain in cases {
            assert_eq!(
                render(plain),
                String::from_utf8_lossy(plain),
                "纯文本或含 \\xc5\\x00 的短载荷必须原样按 lossy 返回"
            );
        }
    }

    #[test]
    fn prefixes_of_measured_payload_never_panic_and_preserve_bytes() {
        let mut pb = Vec::new();
        push_varint(&mut pb, (1 << 3) | 0);
        push_varint(&mut pb, 0);
        let mut fake_pickle = vec![0x80, 0x05, 0x95, 0xfa, 0x01, 0x00, 0xff];
        fake_pickle.resize(517, 0xa3);
        push_len_delimited(&mut pb, 2, &fake_pickle);
        let traceback = concat!(
            "Traceback (most recent call last):\n",
            "  File \"rayrust_test.py\", line 42, in fail\n",
            "ValueError: python-task-boom-t37\n",
        );
        push_len_delimited(&mut pb, 3, traceback.as_bytes());
        let payload = wrap_bin16_with_9byte_header(&pb, [0x00, 0x7f, 0x00, 0x11, 0x00, 0xfe]);

        for end in 0..=payload.len() {
            let prefix = &payload[..end];
            let rendered = render(prefix);
            if end < payload.len() {
                assert_eq!(
                    rendered,
                    String::from_utf8_lossy(prefix),
                    "不完整前缀 len={} 应当完整保留前缀字节（UTF-8 lossy）",
                    end
                );
            } else {
                assert_eq!(rendered, traceback.trim_end());
                assert!(
                    String::from_utf8_lossy(prefix).contains(&rendered),
                    "完整载荷提取出的文本必须来自载荷自身字节"
                );
            }
        }
    }

    #[test]
    fn falls_back_to_lossy_on_malformed_inputs_without_panicking() {
        // 1. 头部声明的整数 N 大于剩余载荷长度
        let oversized_len = [0xcd, 0x04, 0x00, 0x00, 0xff, b'o', b'k'];
        assert_eq!(
            render(&oversized_len),
            String::from_utf8_lossy(&oversized_len)
        );

        // 2. 正文首字节不是 msgpack bin（例如 map 0x81）
        let not_bin = [0x03, 0x00, 0x00, 0x81, 0xa1, b'x', 0xff];
        assert_eq!(render(&not_bin), String::from_utf8_lossy(&not_bin));

        // 3. protobuf 截断（字段 3 声明 50 字节但实际只有 5 字节）
        let mut truncated_pb = Vec::new();
        push_varint(&mut truncated_pb, (3 << 3) | 2);
        push_varint(&mut truncated_pb, 50);
        truncated_pb.extend_from_slice(b"short");
        let truncated_payload = wrap_bin16_with_9byte_header(&truncated_pb, [0xff, 0, 0, 0, 0, 0]);
        assert_eq!(
            render(&truncated_payload),
            String::from_utf8_lossy(&truncated_payload)
        );

        // 4. 字段 3 是非法 UTF-8 字节
        let mut invalid_utf8_pb = Vec::new();
        push_len_delimited(&mut invalid_utf8_pb, 3, &[0xff, 0xfe, b'b', b'a', b'd']);
        let invalid_utf8_payload = wrap_bin16_with_9byte_header(&invalid_utf8_pb, [0u8; 6]);
        assert_eq!(
            render(&invalid_utf8_payload),
            String::from_utf8_lossy(&invalid_utf8_payload)
        );

        // 5. 字段 3 只有空白字符
        let mut whitespace_pb = Vec::new();
        push_len_delimited(&mut whitespace_pb, 3, b"   \n\t  ");
        let whitespace_payload =
            wrap_bin16_with_9byte_header(&whitespace_pb, [0xfe, 0, 0, 0, 0, 0]);
        assert_eq!(
            render(&whitespace_payload),
            String::from_utf8_lossy(&whitespace_payload)
        );
    }

    #[test]
    fn empty_payload_renders_empty_string() {
        assert_eq!(render(&[]), "");
    }

    #[test]
    fn rejects_frames_whose_total_length_is_not_nine_plus_body() {
        let mut pb = Vec::new();
        push_len_delimited(&mut pb, 3, b"ValueError: length-check\n");
        let mut body = vec![0xc4, pb.len() as u8];
        body.extend_from_slice(&pb);

        // 合法帧：9 字节头 + 正文
        let mut ok = vec![body.len() as u8];
        ok.extend_from_slice(&[0u8; 8]);
        ok.extend_from_slice(&body);
        assert_eq!(render(&ok), "ValueError: length-check");

        // 头只有 8 字节：总长小于 9 + 正文长度
        let mut short = vec![body.len() as u8];
        short.extend_from_slice(&[0u8; 7]);
        short.extend_from_slice(&body);
        assert_eq!(render(&short), String::from_utf8_lossy(&short));

        // 头有 10 字节：总长大于 9 + 正文长度
        let mut long = vec![body.len() as u8];
        long.extend_from_slice(&[0u8; 9]);
        long.extend_from_slice(&body);
        assert_eq!(render(&long), String::from_utf8_lossy(&long));
    }

    #[test]
    fn returns_error_message_when_ray_error_info_sets_field3_and_field5() {
        for nested_len in [27usize, 150] {
            let mut nested = Vec::new();
            push_len_delimited(&mut nested, 1, &vec![b'e'; nested_len]);
            let mut pb = Vec::new();
            push_len_delimited(&mut pb, 3, &nested);
            push_len_delimited(
                &mut pb,
                5,
                b"Failed to setup runtime env: module not found\n",
            );
            push_varint(&mut pb, (11 << 3) | 0);
            push_varint(&mut pb, 7);
            let payload = wrap_bin16_with_9byte_header(&pb, [0u8; 6]);
            assert_eq!(
                render(&payload),
                "Failed to setup runtime env: module not found"
            );
        }
    }

    #[test]
    fn nested_field3_without_error_message_is_not_rendered_as_text() {
        let mut nested = Vec::new();
        push_len_delimited(&mut nested, 1, b"nested text");
        let mut pb = Vec::new();
        push_len_delimited(&mut pb, 3, &nested);
        push_varint(&mut pb, (11 << 3) | 0);
        push_varint(&mut pb, 7);
        let payload = wrap_bin16_with_9byte_header(&pb, [0u8; 6]);
        let rendered = render(&payload);
        assert_eq!(rendered, String::from_utf8_lossy(&payload));
        assert_ne!(
            rendered, "nested text",
            "嵌套消息的线格式字节不得当成错误文本"
        );
    }

    #[test]
    fn rejects_printable_text_whose_header_is_not_nine_bytes() {
        // 180 字节的可打印 UTF-8：首字节 `cc 82` 声明正文 130 字节，总长 180 与 9 + 130
        // 对不上，整段回退。
        let mut long_header = Vec::new();
        long_header
            .extend_from_slice("\u{0302}Invalid: failed to parse config at line 42: ".as_bytes());
        long_header.extend_from_slice("\u{0100}*~".as_bytes());
        long_header.extend_from_slice(&[b'A'; 126]);
        assert_eq!((long_header[0], long_header[1]), (0xcc, 0x82));
        assert_ne!(long_header.len(), K_MESSAGEPACK_OFFSET + 0x82);
        assert_eq!(render(&long_header), String::from_utf8_lossy(&long_header));

        // 首字节 `I` 声明正文 73 字节，总长与 9 + 73 对不上的纯文本。
        let mut text = b"Invalid: failed parsing record 42: ".to_vec();
        text.push(0xc4);
        text.push(71);
        text.push(0x2a);
        text.push(69);
        text.extend_from_slice(&[b'a'; 69]);
        assert_eq!(text[0], b'I');
        assert_ne!(text.len(), K_MESSAGEPACK_OFFSET + 73);
        assert_eq!(render(&text), String::from_utf8_lossy(&text));
    }
}
