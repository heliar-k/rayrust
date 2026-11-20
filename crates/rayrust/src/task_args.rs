//! 任务参数的准备：值参数是序列化后的字节，引用参数是那个对象的 28 字节 id 加上
//! 一份替调用方持有的句柄。
//!
//! [`TaskArg`] 描述一个参数位置上的类型：值按 `Serialize` 走，引用按
//! `ObjectRef<T>`（转移句柄）或 `&ObjectRef<T>`（克隆一份）走。宏生成的调用器把
//! 每个参数交给 [`TaskArgs`]，提交之前取出三段：字节、按引用标记、保活集合。
//!
//! 保活集合在提交的 FFI 调用返回之前替调用方持有这些引用。所以引用参数只需要活到
//! 调用器返回（同步）或 future 被丢弃（异步），调用方不必知道提交发生在哪一刻：
//! 异步调用器是 `'static` 的 future，提交发生在第一次轮询，调用方在这之前 drop
//! 掉自己的句柄也不会让对象在提交时失去 owner。序列化、id 长度、来路不明的 id
//! 这些错误先记在 [`TaskArgs`] 里，到提交时统一交给调用方。
//!
//! ## 两种引用参数
//!
//! `ObjectRef<T>` 按值传：句柄连同它的本地引用一起转移进保活集合，不额外加减。
//! `&ObjectRef<T>` 按借用传：`Clone` 加一个本地引用，随保活集合一起释放，调用方
//! 原句柄的计数不受影响。
//!
//! 两种形状过同一道守卫：id 必须是 28 字节，而且必须是本进程当前持有的引用
//! （`ObjectRef::counts_reference`）。第二个条件拦住「保存 id 字节再重建」「自己
//! 拼的 28 字节」「工人交回的 id」这几类句柄：集群模式下把这样的 id 当引用参数交给
//! Ray，`TransformArgs` 的 `GetOwnerAddressOrDie` 会用 `RAY_LOG(FATAL)` 终止进程
//! （无 owner 的 id），本地模式下同一个 id 则永久等待。
//!
//! ## 不支持的形状
//!
//! 工人侧把参数声明成 `ObjectRef<T>`：工人拿到的是那个引用的值（集群模式下
//! core worker 投递值，本地模式下驱动侧先取成值），而 `ObjectRef` 没有
//! `Deserialize`。
//!
//! ```compile_fail
//! use rayrust::prelude::*;
//!
//! #[rayrust::remote]
//! fn takes_ref(r: ObjectRef<i32>) -> i32 {
//!     r.id().len() as i32
//! }
//! # fn main() {}
//! ```
//!
//! 容器里放引用（用 `Vec<ObjectRef<i32>>` 顶替 `Vec<i32>` 参数）：一个参数位置
//! 要么是值 `Vec<i32>`、要么是单个 `ObjectRef<i32>`，`Vec<ObjectRef<i32>>` 两种
//! 形状都不满足，编译停在 `TaskArg` 边界。
//!
//! ```compile_fail
//! use rayrust::prelude::*;
//!
//! #[rayrust::remote]
//! fn sum_list(numbers: Vec<i32>) -> i32 {
//!     numbers.iter().sum()
//! }
//!
//! fn main() -> Result<(), RayError> {
//!     let ray = Ray::connect(&RayConfig::new("127.0.0.1:6379"))?;
//!     let one = ray.put(&1i32)?;
//!     let list = vec![one];
//!     let _ = sum_list_remote(&ray, list)?;
//!     Ok(())
//! }
//! ```
//!
//! ## 用引用顶替一个值参数
//!
//! ```no_run
//! use rayrust::prelude::*;
//!
//! #[rayrust::remote]
//! fn add(a: i32, b: i32) -> i32 {
//!     a + b
//! }
//!
//! fn main() -> Result<(), RayError> {
//!     let ray = Ray::connect(&RayConfig::new("127.0.0.1:6379"))?;
//!     let b = ray.put(&40i32)?;
//!     // 借用：句柄仍归调用方，调用器克隆一份进保活集合
//!     let sum: ObjectRef<i32> = add_remote(&ray, 2, &b)?;
//!     println!("add(2, put(40)) = {}", sum.get()?);
//!
//!     // 按值：句柄转移进保活集合，调用之后不能再使用 `b`
//!     let sum: ObjectRef<i32> = add_remote(&ray, 2, b)?;
//!     println!("add(2, b) = {}", sum.get()?);
//!     Ok(())
//! }
//! ```

use serde::Serialize;

use crate::error::RayError;
use crate::object_ref::ObjectRef;
use crate::serialize::serialize;

/// Ray 2.51.1 的 ObjectID 二进制长度（`src/ray/common/id.h`）。
const OBJECT_ID_LEN: usize = 28;

/// 一次调用里已经准备好的参数。
///
/// 三个位置一一对应：`data[i]` 是第 `i` 个参数交给 C 侧的字节，`is_ref[i]` 说明
/// 它是不是一个对象 id，`held` 是替调用方持有到提交完成的那些引用。值参数可以
/// 序列化失败，引用参数可以带一个长度不对或者来路不明的 id；这些错误先记下来，
/// `into_parts` 时一起交给调用方。
#[derive(Debug, Default)]
pub struct TaskArgs {
    data: Vec<Vec<u8>>,
    is_ref: Vec<bool>,
    held: Vec<ObjectRef<()>>,
    error: Option<RayError>,
}

impl TaskArgs {
    /// 空参数表。
    pub fn new() -> Self {
        TaskArgs::default()
    }

    /// 已经成功准备好的参数个数。
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// 还没有成功准备好的参数。
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// 分成提交通道要的三段（字节、按引用标记、保活集合）；有记下的错误时返回它。
    ///
    /// 错误只交回第一个：后面的参数在第一个错误之后照常准备，但一个调用失败就
    /// 够了，提交也不应该发生。保活集合随返回值移动，调用方负责让它活到提交的
    /// FFI 调用返回。
    pub fn into_parts(mut self) -> Result<(Vec<Vec<u8>>, Vec<bool>, Vec<ObjectRef<()>>), RayError> {
        self.check_layout();
        match self.error {
            Some(error) => Err(error),
            None => Ok((self.data, self.is_ref, self.held)),
        }
    }

    /// 已经有 `data`/`is_ref` 两段的入口用：把现成的参数包成 `TaskArgs`。
    ///
    /// 这样的参数表没有保活集合；两段长度对不上时记下错误，`into_parts` 返回它。
    #[doc(hidden)]
    pub fn from_raw(data: Vec<Vec<u8>>, is_ref: Vec<bool>) -> Self {
        let mut args = TaskArgs {
            data,
            is_ref,
            held: Vec::new(),
            error: None,
        };
        args.check_layout();
        args
    }

    /// 记下第一个错误，后面的错误不再覆盖它。
    fn record(&mut self, error: RayError) {
        if self.error.is_none() {
            self.error = Some(error);
        }
    }

    /// `data` 与 `is_ref` 必须一一对应；对不上就是调用方拼错了参数表。
    fn check_layout(&mut self) {
        if self.data.len() != self.is_ref.len() {
            self.record(RayError::Ffi(format!(
                "task args are inconsistent: {} value buffers but {} pass-by-reference flags",
                self.data.len(),
                self.is_ref.len()
            )));
        }
    }
}

/// 参数位置上的类型：要么是一个值，要么是一个引用（按值或借用）。
///
/// 值参数要求 `Serialize`；引用参数要求 `ObjectRef<T>` 或 `&ObjectRef<T>`。两条
/// 限制：
///
/// 1. 引用必须是本进程当前持有的引用（`put`、任务调用、actor 调用返回的句柄）。
///    从 id 字节重建的、自己拼的、别的进程交回的 28 字节 id 都会被拒成
///    `RayError::Runtime`：这样的 id 交给 Ray 会在 `TransformArgs` 的
///    `GetOwnerAddressOrDie` 里终止进程，本地模式下则永久等待。
/// 2. Rust 任务不能把可用的引用交回来：任务返回的 id 没有嵌套引用，驱动侧也没有
///    这个 id 的 owner 路径，`get_timeout` 得到 `ObjectUnknownOwner`，对象活着
///    也一样。详见 [`crate::ObjectRef`] 模块文档的「作为参数」。
///
/// 拒绝的形状（工人侧声明 `ObjectRef<T>`、容器里放引用）在模块文档里。
pub trait TaskArg<T> {
    /// 把 `self` 作为一个 `T` 类型的参数放进 `args`。
    fn push_arg(self, args: &mut TaskArgs);
}

impl<T: Serialize> TaskArg<T> for T {
    fn push_arg(self, args: &mut TaskArgs) {
        match serialize(&self) {
            Ok(bytes) => {
                args.data.push(bytes);
                args.is_ref.push(false);
            }
            Err(error) => args.record(error),
        }
    }
}

impl<T> TaskArg<T> for ObjectRef<T> {
    fn push_arg(self, args: &mut TaskArgs) {
        if let Some(error) = reference_arg_error(&self) {
            args.record(error);
            return;
        }
        args.data.push(self.id().to_vec());
        args.is_ref.push(true);
        // 句柄连同它的本地引用一起转移，不额外加减。
        args.held.push(self.cast());
    }
}

impl<'a, T> TaskArg<T> for &'a ObjectRef<T> {
    fn push_arg(self, args: &mut TaskArgs) {
        if let Some(error) = reference_arg_error(self) {
            args.record(error);
            return;
        }
        args.data.push(self.id().to_vec());
        args.is_ref.push(true);
        // 克隆加一个本地引用，随保活集合一起释放；调用方原句柄不动。
        args.held.push(self.clone().cast());
    }
}

/// 一个引用参数要先过的两道关：id 长度、本进程是否持有它。
fn reference_arg_error<T>(obj: &ObjectRef<T>) -> Option<RayError> {
    if obj.id().len() != OBJECT_ID_LEN {
        // 长度不对的 id 交给 C++ 侧会被 `ObjectID::FromBinary` 的 `RAY_CHECK`
        // 终止进程（`src/ray/common/id.h:430-440`），在这里先拦成错误。
        return Some(RayError::Ffi(format!(
            "object id must be {} bytes, got {} bytes",
            OBJECT_ID_LEN,
            obj.id().len()
        )));
    }
    if !obj.counts_reference() {
        return Some(RayError::Runtime(format!(
            "object id {} is not a reference this process holds; pass a handle returned by put or by a task/actor call",
            obj.id_hex()
        )));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 发布/撤掉代次是进程级状态，测试线程并行跑会互相踩：拿 object_ref 里那把锁
    /// 把自己串起来。
    fn epoch_guard() -> std::sync::MutexGuard<'static, ()> {
        crate::object_ref::EPOCH_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[test]
    fn value_args_are_serialized_and_not_marked_as_refs() {
        let mut args = TaskArgs::new();
        TaskArg::<i32>::push_arg(7i32, &mut args);
        TaskArg::<String>::push_arg("hi".to_string(), &mut args);
        assert_eq!(args.len(), 2);
        let (data, is_ref, held) = args.into_parts().expect("两个值参数都序列化成功");
        assert_eq!(data.len(), 2);
        assert_eq!(is_ref, vec![false, false]);
        assert!(held.is_empty(), "值参数不进保活集合");
        assert_eq!(crate::serialize::deserialize::<i32>(&data[0]).unwrap(), 7);
        assert_eq!(
            crate::serialize::deserialize::<String>(&data[1]).unwrap(),
            "hi"
        );
    }

    /// 按值传的引用把句柄本身转移进保活集合：不额外加引用，所以不需要运行时也能跑。
    #[test]
    fn a_by_value_reference_is_held_in_the_slot() {
        let _guard = epoch_guard();
        let epoch = crate::object_ref::publish_live_epoch();
        let id = vec![3u8; OBJECT_ID_LEN];
        let obj: ObjectRef<i32> = ObjectRef::from_call_id(id.clone());
        let mut args = TaskArgs::new();
        TaskArg::<i32>::push_arg(obj, &mut args);
        let parts = args.into_parts();
        // 先撤代次：下面 drop 保活句柄时不再进入 Ray（测试进程没有运行时）。
        crate::object_ref::retract_live_epoch();
        assert!(epoch != 0, "代次表没有发布成功");
        let (data, is_ref, held) = parts.expect("本进程持有的 28 字节引用可以当引用参数");
        assert_eq!(data, vec![id.clone()]);
        assert_eq!(is_ref, vec![true]);
        assert_eq!(held.len(), 1);
        assert_eq!(held[0].id(), id.as_slice());
        drop(held);
    }

    /// 一个序列化一定失败的值：非 UTF-8 的路径，`Path` 的 `Serialize` 直接报错。
    fn unserializable_path() -> std::path::PathBuf {
        use std::os::unix::ffi::OsStrExt;
        std::path::PathBuf::from(std::ffi::OsStr::from_bytes(b"\xff"))
    }

    #[test]
    fn a_serialization_error_is_kept_until_into_parts() {
        let mut args = TaskArgs::new();
        TaskArg::<std::path::PathBuf>::push_arg(unserializable_path(), &mut args);
        assert!(args.is_empty(), "序列化失败时不该留下半个参数");
        match args.into_parts() {
            Err(RayError::Serialization(_)) => {}
            Err(other) => panic!("返回了别的错误变体: {}", other),
            Ok(_) => panic!("序列化失败却拿到了参数"),
        }
    }

    #[test]
    fn a_27_byte_id_is_rejected_as_a_ref_arg() {
        let obj: ObjectRef<i32> = ObjectRef::from(vec![0u8; OBJECT_ID_LEN - 1]);
        let mut by_ref = TaskArgs::new();
        TaskArg::<i32>::push_arg(&obj, &mut by_ref);
        let mut by_value = TaskArgs::new();
        TaskArg::<i32>::push_arg(obj, &mut by_value);
        for (label, args) in [("借用", by_ref), ("按值", by_value)] {
            match args.into_parts() {
                Err(RayError::Ffi(msg)) => assert_eq!(
                    msg, "object id must be 28 bytes, got 27 bytes",
                    "{} 的错误消息不对",
                    label
                ),
                Err(other) => panic!("{} 返回了别的错误变体: {}", label, other),
                Ok(_) => panic!("{}：27 字节的 id 被当成了引用参数", label),
            }
        }
    }

    #[test]
    fn a_rebuilt_28_byte_id_is_rejected_as_a_ref_arg() {
        let obj: ObjectRef<i32> = ObjectRef::from(vec![7u8; OBJECT_ID_LEN]);
        let mut by_ref = TaskArgs::new();
        TaskArg::<i32>::push_arg(&obj, &mut by_ref);
        let mut by_value = TaskArgs::new();
        TaskArg::<i32>::push_arg(obj, &mut by_value);
        for (label, args) in [("借用", by_ref), ("按值", by_value)] {
            match args.into_parts() {
                Err(RayError::Runtime(msg)) => {
                    assert!(
                        msg.contains("is not a reference this process holds"),
                        "{} 的消息看不出原因: {}",
                        label,
                        msg
                    )
                }
                Err(other) => panic!("{} 返回了别的错误变体: {}", label, other),
                Ok(_) => panic!("{}：来路不明的 28 字节 id 被当成了引用参数", label),
            }
        }
    }

    #[test]
    fn the_first_error_wins() {
        let obj: ObjectRef<i32> = ObjectRef::from(vec![0u8; 27]);
        let mut args = TaskArgs::new();
        TaskArg::<i32>::push_arg(&obj, &mut args);
        TaskArg::<std::path::PathBuf>::push_arg(unserializable_path(), &mut args);
        match args.into_parts() {
            Err(RayError::Ffi(_)) => {}
            Err(other) => panic!("返回了别的错误变体: {}", other),
            Ok(_) => panic!("两个参数都坏了却拿到了参数"),
        }
    }

    #[test]
    fn from_raw_never_carries_an_error() {
        let args = TaskArgs::from_raw(vec![vec![1, 2, 3]], vec![true]);
        assert_eq!(args.len(), 1);
        assert!(!args.is_empty());
        let (data, is_ref, held) = args.into_parts().expect("from_raw 的两段长度一致");
        assert_eq!(data, vec![vec![1, 2, 3]]);
        assert_eq!(is_ref, vec![true]);
        assert!(held.is_empty(), "from_raw 不带保活集合");
    }

    #[test]
    fn from_raw_with_mismatched_lengths_is_rejected() {
        let args = TaskArgs::from_raw(vec![vec![1], vec![2]], vec![true]);
        match args.into_parts() {
            Err(RayError::Ffi(msg)) => assert!(
                msg.contains("2 value buffers but 1 pass-by-reference flags"),
                "消息看不出长度对不上: {}",
                msg
            ),
            Err(other) => panic!("返回了别的错误变体: {}", other),
            Ok(_) => panic!("两段长度不一致却分出了参数"),
        }
    }
}
