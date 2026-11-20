//! 本进程发出去的 actor handle：有哪些、各是什么语言、各自的创建描述符是哪一种。
//!
//! 上游没有非致命的「这个 id 本进程有 handle 吗」查询。`ActorManager::GetActorHandle`
//! （`src/ray/core_worker/actor_manager.cc:52-59`）查不到就 `RAY_CHECK` 终止进程，
//! `RAY_CATCH` 抓不到；非致命的 `CheckActorHandleExists`（同文件 `:117`）与
//! `GetActorHandleIfExists`（`:122`）挂在 `CoreWorker` 的私有成员
//! （`src/ray/core_worker/core_worker.h:1825`）上，`cpp/src/ray` 拿不到实例。所以
//! 「这个 handle 是本进程发出的吗」只能由 crate 自己记，登记表就是这份记录。
//!
//! 表里只有 crate 自己发出的四类 handle：[`crate::Ray::actor_create`]、
//! [`crate::Ray::actor_create_python`]、[`crate::Ray::get_actor`] 与
//! [`crate::ActorHandle::from_serialized`]。裸 16 字节 id（垃圾字节、重连之前留下的
//! 旧 handle、16 个 `0xff` 的 nil id）不在表里，[`crate::Ray::actor_call`]、
//! [`crate::Ray::actor_call_async`]、[`crate::Ray::kill_actor`] 与
//! [`crate::ActorHandle::serialize`] 因此能在进 SDK 之前拒掉它们
//! （[`crate::Ray::actor_call_python`] 在本地模式整段跳过 crate 侧检查，见它的文档）。
//! 直接用原始 C ABI（例如 `rayrust::sys::ray_actor_create_with_options`）建出来的
//! actor 也不在表里：集群模式可以用 `ray_actor_handle_serialize` 拿到字节再走
//! [`crate::ActorHandle::from_serialized`] 带进来，本地模式没有回填路径，只能继续用
//! C ABI 调用。这些 id 会到达上游 RAY_CHECK 的三条路径：
//! `cpp/src/ray/runtime/task/native_task_submitter.cc:81`（C++ 分支）、
//! `cpp/src/ray/runtime/abstract_ray_runtime.cc:202-211`（Python 分支先读 actor
//! handle 再建调用），以及 `ActorManager::GetActorHandle` 自己的 RAY_CHECK
//! （`actor_manager.cc:52-59`）。
//!
//! 语言与创建描述符类型一并记在这里，因为上游只在序列化字节里给出它们：语言是
//! `rpc.ActorHandle.actor_language`（`src/ray/protobuf/core_worker.proto:42`，字段 5），
//! 取值 `PYTHON = 0`、`JAVA = 1`、`CPP = 2`（`src/ray/protobuf/common.proto:26-30`）；
//! 描述符类型是字段 6 里那个 oneof 成员的字段号
//! （`src/ray/protobuf/common.proto:158-164`），java 1、python 2、cpp 3。两个调用入口
//! 按语言判定 id 的语言与入口要求的语言是否一致：语言不匹配的调用要么让 C++ 工人终止
//! （`cpp/src/ray/runtime/task/task_executor.cc:145` 的
//! `RAY_CHECK(ray_function.GetLanguage() == ray::Language::CPP)`），要么让 Python 工人
//! 在错误的函数描述符上失败退出。[`crate::Ray::actor_call_python`] 还要求描述符是
//! Python：那条路径把 handle 的创建描述符按 `PythonFunctionDescriptor` 读
//! （`cpp/src/ray/runtime/abstract_ray_runtime.cc:202-211`），描述符缺席或类型不符时
//! 读越过对象末尾。
//!
//! 同一个 id 登记两次时保留首次登记的那一份，与上游 `ActorManager::AddActorHandle` 的
//! `actor_handles_.emplace(...)` 一致（`src/ray/core_worker/actor_manager.cc:179`）：一个
//! id 相同、语言字段说谎的载荷改变不了已经发出去的 handle 被哪些入口接受。
//!
//! 登记表随运行时清空（[`clear`]，由 `Drop for RayInner` 调用）：上一个运行时的 handle
//! 在下一个运行时里查不到。

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

/// `ray::ActorID` 的长度：12 个唯一字节 + 4 字节 JobID（`src/ray/common/id.h:124-142`）。
pub(crate) const ACTOR_ID_LEN: usize = 16;

/// `rpc.ActorHandle.actor_language` 的取值（`src/ray/protobuf/common.proto:26-30`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActorLanguage {
    Python,
    Java,
    Cpp,
}

impl ActorLanguage {
    /// 上游的整数编码。
    pub(crate) fn code(self) -> i32 {
        match self {
            ActorLanguage::Python => 0,
            ActorLanguage::Java => 1,
            ActorLanguage::Cpp => 2,
        }
    }

    /// 整数编码对应的语言；这个 crate 不认识的码返回 `None`。
    pub(crate) fn from_code(code: i32) -> Option<ActorLanguage> {
        match code {
            0 => Some(ActorLanguage::Python),
            1 => Some(ActorLanguage::Java),
            2 => Some(ActorLanguage::Cpp),
            _ => None,
        }
    }

    /// 错误文本里用的名字。
    pub(crate) fn name(self) -> &'static str {
        match self {
            ActorLanguage::Python => "Python",
            ActorLanguage::Java => "Java",
            ActorLanguage::Cpp => "C++",
        }
    }
}

/// 这个 crate 自己建出来的 actor 的创建描述符类型，取值是 `rpc.FunctionDescriptor`
/// 那个 oneof 成员的字段号（`src/ray/protobuf/common.proto:158-164`）。java（1）只可能
/// 来自 [`crate::ActorHandle::from_serialized`] 的载荷，crate 自己建不出来。
pub(crate) const CREATION_DESCRIPTOR_PYTHON: i32 = 2;
pub(crate) const CREATION_DESCRIPTOR_CPP: i32 = 3;

/// 一个登记项：上游给出的语言码与创建描述符类型码。
#[derive(Debug, Clone, Copy)]
pub(crate) struct Registration {
    /// `actor_language` 的原值。未知取值照样保存：proto3 的 enum 字段未来可能给出
    /// 这个 crate 不认识的码，那种 id 的调用要报出实际语言码，与「没登记」区分开。
    pub(crate) language: i32,
    /// 创建描述符的 oneof 字段号，0 表示 handle 里没有描述符。
    pub(crate) creation_descriptor: i32,
}

/// actor id → 登记项。
static ACTOR_REGISTRATIONS: LazyLock<Mutex<HashMap<[u8; ACTOR_ID_LEN], Registration>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// 记下一个 crate 发出的 handle 的语言与创建描述符类型。
///
/// 长度不是 [`ACTOR_ID_LEN`] 就什么也不做：调用方都已经检查过宽度。
pub(crate) fn record(actor_id: &[u8], language: ActorLanguage, creation_descriptor: i32) {
    record_codes(actor_id, language.code(), creation_descriptor)
}

/// 记下 C 侧从 `rpc.ActorHandle` 里读出的语言码与创建描述符类型，不认识的取值照样
/// 保存。
///
/// 长度不是 [`ACTOR_ID_LEN`] 就什么也不做。同一个 id 已经登记过时保留首次登记的那
/// 一份：与上游 `ActorManager::AddActorHandle` 的 `actor_handles_.emplace(...)` 一致
/// （`src/ray/core_worker/actor_manager.cc:179`）。
pub(crate) fn record_codes(actor_id: &[u8], language: i32, creation_descriptor: i32) {
    if actor_id.len() != ACTOR_ID_LEN {
        return;
    }
    let key: [u8; ACTOR_ID_LEN] = actor_id
        .try_into()
        .expect("the length was checked to be ACTOR_ID_LEN");
    ACTOR_REGISTRATIONS
        .lock()
        .expect("actor registry poisoned")
        .entry(key)
        .or_insert(Registration {
            language,
            creation_descriptor,
        });
}

/// 这个 actor id 登记的语言码与创建描述符类型；没登记过（或者长度不是
/// [`ACTOR_ID_LEN`]）返回 `None`。
pub(crate) fn registration_of(actor_id: &[u8]) -> Option<Registration> {
    if actor_id.len() != ACTOR_ID_LEN {
        return None;
    }
    ACTOR_REGISTRATIONS
        .lock()
        .expect("actor registry poisoned")
        .get(actor_id)
        .copied()
}

/// 清空登记表。运行时的 `Drop` 调用它，旧运行时的 handle 因此在新运行时里查不到。
pub(crate) fn clear() {
    ACTOR_REGISTRATIONS
        .lock()
        .expect("actor registry poisoned")
        .clear();
}
