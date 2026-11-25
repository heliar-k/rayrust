/// Private ABI declarations, build-time layout assertions, runtime `dlsym`
/// symbol resolution, and ELF `kRayVersion` verification for `rayrust-sys`.
///
/// Public headers shipped in `ray-cpp` (`include/ray/api.h`, `include/ray/api/*.h`)
/// omit internal classes (`Buffer`, `ObjectID`, `Status`, `RayObject`,
/// `CoreWorker`, `GlobalStateAccessor`, `ProcessHelper`, `AbstractRayRuntime`)
/// and internal functions (`GetFunctionManager`, `CoreWorkerProcess`, etc.).
/// This header isolates every non-public declaration in one place:
/// - `_GLIBCXX_USE_CXX11_ABI` is probed from `libray_api.so` by `build.rs` and
///   verified here with `static_assert`.
/// - Minimal layout stand-ins carry compile-time `static_assert` checks.
/// - All 12 non-public `libray_api.so` symbols are resolved at runtime via
///   `dlsym`, leaving zero undefined private C++ symbols in `libray_c.a`.
/// - On Linux, `_ZL11kRayVersion` (`src/ray/common/constants.h:68`, referenced
///   at `src/ray/core_worker/core_worker_process.cc:780-785`) is read from the
///   loaded `libray_api.so` ELF64 `.symtab` section and compared against
///   `RAYRUST_BUILD_RAY_VERSION`.

#pragma once

#if !defined(RAYRUST_GLIBCXX_CXX11_ABI)
#error "RAYRUST_GLIBCXX_CXX11_ABI must be defined by build.rs"
#endif

#if !defined(RAYRUST_BUILD_RAY_VERSION)
#error "RAYRUST_BUILD_RAY_VERSION must be defined by build.rs"
#endif

#include <ray/api.h>
#include <ray/api/function_manager.h>
#include <ray/api/ray_exception.h>
#include <ray/api/ray_runtime.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <dlfcn.h>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <string>
#include <type_traits>
#include <vector>

#ifdef __linux__
#include <elf.h>
#endif

#ifdef __GLIBCXX__
static_assert(
    _GLIBCXX_USE_CXX11_ABI == RAYRUST_GLIBCXX_CXX11_ABI,
    "_GLIBCXX_USE_CXX11_ABI does not match RAYRUST_GLIBCXX_CXX11_ABI probed by build.rs");
#if INTPTR_MAX == INT64_MAX
#if RAYRUST_GLIBCXX_CXX11_ABI == 0
static_assert(sizeof(std::string) == sizeof(void *),
              "legacy libstdc++ std::string must be pointer-sized on 64-bit targets");
#elif RAYRUST_GLIBCXX_CXX11_ABI == 1
static_assert(sizeof(std::string) == 4 * sizeof(void *),
              "cxx11 libstdc++ std::string must be 32 bytes on 64-bit targets");
#else
#error "RAYRUST_GLIBCXX_CXX11_ABI must be 0 or 1"
#endif
#endif
#endif

// `RayIntentionalSystemExitException` 定义在 `cpp/src/ray/runtime/abstract_ray_runtime.h`，
// wheel 只带 `include/ray/api/*`，这里按上游定义声明：继承 `RayException`，无新增数据成员。
//
// libstdc++ 在两份 `type_info` 地址不同且 `__GXX_MERGED_TYPEINFO_NAMES == 0` 时按 mangled name
// `N3ray8internal33RayIntentionalSystemExitExceptionE` 匹配 `GetExecuteResult` 的
// `catch (RayIntentionalSystemExitException &)`（`cpp/src/ray/runtime/task/task_executor.cc:97-100`）；
// 下面的三条 `static_assert` 约束基类、大小与对齐。
namespace ray {
namespace internal {
class RayIntentionalSystemExitException : public RayException {
public:
    explicit RayIntentionalSystemExitException(const std::string &msg) : RayException(msg) {}
};

static_assert(std::is_base_of_v<RayException, RayIntentionalSystemExitException>,
              "RayIntentionalSystemExitException must derive from ray::internal::RayException");
static_assert(sizeof(RayIntentionalSystemExitException) == sizeof(RayException),
              "RayIntentionalSystemExitException must match sizeof(RayException)");
static_assert(alignof(RayIntentionalSystemExitException) == alignof(RayException),
              "RayIntentionalSystemExitException must match alignof(RayException)");
}  // namespace internal
}  // namespace ray

namespace ray {

/// Stand-in for `ray::Buffer` (`src/ray/common/buffer.h:30-57`): pure virtual
/// base with four const accessors and a virtual destructor.
class __attribute__((visibility("hidden"))) Buffer {
public:
    Buffer() = default;
    virtual uint8_t *Data() const = 0;
    virtual size_t Size() const = 0;
    virtual bool OwnsData() const = 0;
    virtual bool IsPlasmaBuffer() const = 0;
    virtual ~Buffer() = default;
    Buffer(const Buffer &) = delete;
    Buffer &operator=(const Buffer &) = delete;
};

static_assert(sizeof(ray::Buffer) == sizeof(void *),
              "ray::Buffer stand-in does not match the SDK layout");

#pragma pack(push, 1)
/// Stand-in for `ray::ObjectID` (`src/ray/common/id.h:85`, `:369-370`, `:516-525`):
/// `BaseID<ObjectID>` under `#pragma pack(1)` with `mutable size_t hash_` at
/// byte offset 0 followed by `uint8_t id_[28]` at byte offset `sizeof(size_t)`.
class __attribute__((visibility("hidden"))) ObjectID {
public:
    static constexpr size_t kLength = 28;

    constexpr ObjectID() noexcept : hash_(0), id_{} {}

    ObjectID(const char *data, size_t len) noexcept : hash_(0), id_{} {
        std::memcpy(id_, data, len);
    }

    std::string Binary() const {
        return std::string(reinterpret_cast<const char *>(id_), kLength);
    }

    static constexpr bool verify_field_offsets() noexcept {
        return offsetof(ObjectID, hash_) == 0 &&
               offsetof(ObjectID, id_) == sizeof(size_t);
    }

private:
    mutable size_t hash_;
    uint8_t id_[kLength];
};
#pragma pack(pop)

static_assert(std::is_standard_layout_v<ray::ObjectID>,
              "ray::ObjectID stand-in must have standard layout");
static_assert(alignof(ray::ObjectID) == 1,
              "ray::ObjectID stand-in must be 1-byte packed");
static_assert(sizeof(ray::ObjectID) == ray::ObjectID::kLength + sizeof(size_t),
              "ray::ObjectID stand-in size must equal 28 + sizeof(size_t)");
static_assert(ray::ObjectID::verify_field_offsets(),
              "ray::ObjectID stand-in field offsets must place hash_ at 0 and id_ at sizeof(size_t)");

/// Stand-in for `ray::Status` (`src/ray/common/status.h:285-524`).
/// Keeping a user-declared non-trivial destructor preserves the Itanium C++ ABI
/// hidden return-slot convention (`sret` in `%rdi`) expected by `CoreWorker::Put`
/// and `CoreWorker::CancelTask`, while `~Status()` releases `state_` through
/// `dlsym`-resolved `_ZN3ray6StatusD1Ev` without emitting an undefined symbol.
class __attribute__((visibility("hidden"))) Status {
public:
    Status() noexcept : state_(nullptr) {}
    inline ~Status();

    Status(const Status &) = delete;
    Status &operator=(const Status &) = delete;

    bool ok() const noexcept { return state_ == nullptr; }
    inline std::string ToString() const;

private:
    struct State;
    State *state_;
};

static_assert(sizeof(ray::Status) == sizeof(void *),
              "ray::Status stand-in does not match the SDK layout");
static_assert(!std::is_trivially_destructible_v<ray::Status>,
              "ray::Status must remain non-trivially destructible for Itanium sret ABI");

struct __attribute__((visibility("hidden"))) OpaqueObjectReference {};

/// Stand-in for `ray::RayObject` (`src/ray/common/ray_object.h:28-177`, fields
/// at `:163-176`), whose tail fields after `creation_time_nanos_` are selected
/// by `build.rs` from `libray_api.so` symbols (112 bytes with enum on Ray 2.49.0
/// and 2.51.1, 136 bytes with two `std::optional<std::string>` fields on Ray
/// 2.55.0 and 2.60.0).
class __attribute__((visibility("hidden"))) RayObject {
public:
    RayObject(std::shared_ptr<Buffer> data, std::shared_ptr<Buffer> metadata)
        : data_(std::move(data)), metadata_(std::move(metadata)) {}

    ~RayObject() = default;

private:
    std::shared_ptr<Buffer> data_;
    std::function<const std::shared_ptr<Buffer>()> data_factory_ = nullptr;
    std::shared_ptr<Buffer> metadata_;
    std::vector<OpaqueObjectReference> nested_refs_;
    bool has_data_copy_ = false;
    bool accessed_ = false;
    int64_t creation_time_nanos_ = 0;
#if defined(RAYRUST_RAY_OBJECT_LAYOUT_OPTIONAL_STR)
    std::optional<std::string> tensor_transport_;
    std::optional<std::string> direct_transport_metadata_;
#elif defined(RAYRUST_RAY_OBJECT_LAYOUT_ENUM)
    int32_t tensor_transport_ = 0;
#else
#error "ray::RayObject layout macro not defined by build.rs"
#endif
};

#if defined(RAYRUST_RAY_OBJECT_LAYOUT_OPTIONAL_STR)
static_assert(sizeof(ray::RayObject) == 136,
              "ray::RayObject stand-in size must be 136 bytes for optional<string> layout");
#elif defined(RAYRUST_RAY_OBJECT_LAYOUT_ENUM)
static_assert(sizeof(ray::RayObject) == 112,
              "ray::RayObject stand-in size must be 112 bytes for enum layout");
#endif

}  // namespace ray

namespace ray::core {
class CoreWorker;
}  // namespace ray::core

namespace ray::gcs {
class GlobalStateAccessor;
}  // namespace ray::gcs

namespace ray::internal {
/// `ProcessHelper` (`cpp/src/ray/util/process_helper.h:28-52`) has no virtual
/// methods and no non-static data members.
class ProcessHelper {};

/// `AbstractRayRuntime` (`cpp/src/ray/runtime/abstract_ray_runtime.h:41-131`)
/// inherits publicly from `RayRuntime`.
class AbstractRayRuntime : public RayRuntime {};
}  // namespace ray::internal

namespace rayrust_abi {

/// Self-contained `ray::Buffer` backed by `std::vector<uint8_t>`.
class __attribute__((visibility("hidden"))) OwnedRayBuffer final : public ray::Buffer {
public:
    OwnedRayBuffer(const uint8_t *src, size_t len) : bytes_(src, src + len) {}

    uint8_t *Data() const override {
        return const_cast<uint8_t *>(bytes_.data());
    }
    size_t Size() const override { return bytes_.size(); }
    bool OwnsData() const override { return true; }
    bool IsPlasmaBuffer() const override { return false; }

private:
    std::vector<uint8_t> bytes_;
};

// ─── Private symbol table (12 symbols in libray_api.so) ───────

inline constexpr const char *kSymGetFunctionManager =
    "_ZN3ray8internal18GetFunctionManagerEv";
inline constexpr const char *kHumanGetFunctionManager =
    "ray::internal::GetFunctionManager()";

inline constexpr const char *kSymAbstractRayRuntimeGetInstance =
    "_ZN3ray8internal18AbstractRayRuntime11GetInstanceEv";
inline constexpr const char *kHumanAbstractRayRuntimeGetInstance =
    "ray::internal::AbstractRayRuntime::GetInstance()";

inline constexpr const char *kSymAbstractRayRuntimeGetGsa =
    "_ZN3ray8internal18AbstractRayRuntime22GetGlobalStateAccessorEv";
inline constexpr const char *kHumanAbstractRayRuntimeGetGsa =
    "ray::internal::AbstractRayRuntime::GetGlobalStateAccessor()";

#if RAYRUST_GLIBCXX_CXX11_ABI == 0
inline constexpr const char *kSymProcessHelperCreateGsa =
    "_ZN3ray8internal13ProcessHelper25CreateGlobalStateAccessorERKSsi";
inline constexpr const char *kSymGlobalStateAccessorGetInternalKV =
    "_ZN3ray3gcs19GlobalStateAccessor13GetInternalKVERKSsS3_";
inline constexpr const char *kSymStatusToString =
    "_ZNK3ray6Status8ToStringEv";
#else
inline constexpr const char *kSymProcessHelperCreateGsa =
    "_ZN3ray8internal13ProcessHelper25CreateGlobalStateAccessorERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEi";
inline constexpr const char *kSymGlobalStateAccessorGetInternalKV =
    "_ZN3ray3gcs19GlobalStateAccessor13GetInternalKVERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES9_";
inline constexpr const char *kSymStatusToString =
    "_ZNK3ray6Status8ToStringB5cxx11Ev";
#endif

inline constexpr const char *kHumanProcessHelperCreateGsa =
    "ray::internal::ProcessHelper::CreateGlobalStateAccessor(const std::string &, int)";
inline constexpr const char *kHumanGlobalStateAccessorGetInternalKV =
    "ray::gcs::GlobalStateAccessor::GetInternalKV(const std::string &, const std::string &)";

inline constexpr const char *kSymGlobalStateAccessorDtor =
    "_ZN3ray3gcs19GlobalStateAccessorD1Ev";
inline constexpr const char *kHumanGlobalStateAccessorDtor =
    "ray::gcs::GlobalStateAccessor::~GlobalStateAccessor()";

inline constexpr const char *kSymCoreWorkerProcessIsInitialized =
    "_ZN3ray4core17CoreWorkerProcess13IsInitializedEv";
inline constexpr const char *kHumanCoreWorkerProcessIsInitialized =
    "ray::core::CoreWorkerProcess::IsInitialized()";

inline constexpr const char *kSymCoreWorkerProcessGetCoreWorker =
    "_ZN3ray4core17CoreWorkerProcess13GetCoreWorkerEv";
inline constexpr const char *kHumanCoreWorkerProcessGetCoreWorker =
    "ray::core::CoreWorkerProcess::GetCoreWorker()";

inline constexpr const char *kSymCoreWorkerPut =
    "_ZN3ray4core10CoreWorker3PutERKNS_9RayObjectERKSt6vectorINS_8ObjectIDESaIS6_EEPS6_";
inline constexpr const char *kHumanCoreWorkerPut =
    "ray::core::CoreWorker::Put(const ray::RayObject &, const std::vector<ray::ObjectID> &, ray::ObjectID *)";

inline constexpr const char *kSymCoreWorkerCancelTask =
    "_ZN3ray4core10CoreWorker10CancelTaskERKNS_8ObjectIDEbb";
inline constexpr const char *kHumanCoreWorkerCancelTask =
    "ray::core::CoreWorker::CancelTask(const ray::ObjectID &, bool, bool)";

inline constexpr const char *kSymStatusDtor = "_ZN3ray6StatusD1Ev";
inline constexpr const char *kHumanStatusDtor = "ray::Status::~Status()";
inline constexpr const char *kHumanStatusToString = "ray::Status::ToString() const";

struct PrivateSymbolDescriptor {
    const char *mangled;
    const char *human;
};

inline constexpr PrivateSymbolDescriptor kPrivateSymbols[12] = {
    {kSymGetFunctionManager, kHumanGetFunctionManager},
    {kSymAbstractRayRuntimeGetInstance, kHumanAbstractRayRuntimeGetInstance},
    {kSymAbstractRayRuntimeGetGsa, kHumanAbstractRayRuntimeGetGsa},
    {kSymProcessHelperCreateGsa, kHumanProcessHelperCreateGsa},
    {kSymGlobalStateAccessorGetInternalKV, kHumanGlobalStateAccessorGetInternalKV},
    {kSymGlobalStateAccessorDtor, kHumanGlobalStateAccessorDtor},
    {kSymCoreWorkerProcessIsInitialized, kHumanCoreWorkerProcessIsInitialized},
    {kSymCoreWorkerProcessGetCoreWorker, kHumanCoreWorkerProcessGetCoreWorker},
    {kSymCoreWorkerPut, kHumanCoreWorkerPut},
    {kSymCoreWorkerCancelTask, kHumanCoreWorkerCancelTask},
    {kSymStatusDtor, kHumanStatusDtor},
    {kSymStatusToString, kHumanStatusToString},
};

inline const char *human_name_for_symbol(const char *mangled_name) {
    for (const auto &entry : kPrivateSymbols) {
        if (std::strcmp(entry.mangled, mangled_name) == 0) {
            return entry.human;
        }
    }
    return mangled_name;
}

struct SdkLibraryInfo {
    void *handle = RTLD_DEFAULT;
    std::string path;
    bool has_file_path = false;
};

inline const SdkLibraryInfo &sdk_library() {
    static SdkLibraryInfo info = []() {
        SdkLibraryInfo out;
        Dl_info dl_info{};
        if (dladdr(reinterpret_cast<const void *>(&ray::IsInitialized), &dl_info) != 0 &&
            dl_info.dli_fname != nullptr && dl_info.dli_fname[0] != '\0') {
            out.path = dl_info.dli_fname;
            out.has_file_path = true;
            void *opened = dlopen(dl_info.dli_fname, RTLD_LAZY | RTLD_NOLOAD);
            if (opened != nullptr) {
                out.handle = opened;
            }
        } else {
            out.path = "<unknown libray_api.so>";
        }
        return out;
    }();
    return info;
}

inline void *resolve_private_symbol(const char *mangled_name,
                                    const char *human_name,
                                    std::string &err_out) {
    const auto &lib = sdk_library();
    void *sym = dlsym(lib.handle, mangled_name);
    if (sym == nullptr && lib.handle != RTLD_DEFAULT) {
        sym = dlsym(RTLD_DEFAULT, mangled_name);
    }
    if (sym == nullptr) {
        err_out = std::string("missing private Ray symbol ") + human_name + " (" +
                  mangled_name + ") in loaded SDK library " + lib.path +
                  " (built against Ray C++ SDK " RAYRUST_BUILD_RAY_VERSION ")";
        return nullptr;
    }
    return sym;
}

// ─── Typed function pointer resolvers (Itanium C++ ABI) ───────

using GetFunctionManagerFn = ray::internal::FunctionManager &(*)();
using AbstractRayRuntimeGetInstanceFn =
    std::shared_ptr<ray::internal::AbstractRayRuntime> (*)();
using AbstractRayRuntimeGetGsaFn =
    const std::unique_ptr<ray::gcs::GlobalStateAccessor> &(*)(
        ray::internal::AbstractRayRuntime *);
using GlobalStateAccessorGetInternalKVFn =
    std::unique_ptr<std::string> (*)(ray::gcs::GlobalStateAccessor *,
                                     const std::string &,
                                     const std::string &);
using GlobalStateAccessorDtorFn = void (*)(ray::gcs::GlobalStateAccessor *);
using CoreWorkerProcessIsInitializedFn = bool (*)();
using CoreWorkerProcessGetCoreWorkerFn = ray::core::CoreWorker &(*)();
using CoreWorkerPutFn = ray::Status (*)(ray::core::CoreWorker *,
                                        const ray::RayObject &,
                                        const std::vector<ray::ObjectID> &,
                                        ray::ObjectID *);
using CoreWorkerCancelTaskFn = ray::Status (*)(ray::core::CoreWorker *,
                                               const ray::ObjectID &,
                                               bool,
                                               bool);
using StatusDtorFn = void (*)(ray::Status *);
using StatusToStringFn = std::string (*)(const ray::Status *);

template <typename FnPtr>
inline FnPtr resolve_cached(std::atomic<FnPtr> &slot,
                            const char *mangled,
                            const char *human,
                            std::string &err_out) {
    FnPtr cached = slot.load(std::memory_order_acquire);
    if (cached != nullptr) {
        return cached;
    }
    void *raw = resolve_private_symbol(mangled, human, err_out);
    if (raw == nullptr) {
        return nullptr;
    }
    FnPtr resolved = reinterpret_cast<FnPtr>(raw);
    slot.store(resolved, std::memory_order_release);
    return resolved;
}

inline GetFunctionManagerFn resolve_get_function_manager(std::string &err_out) {
    static std::atomic<GetFunctionManagerFn> slot{nullptr};
    return resolve_cached(
        slot, kSymGetFunctionManager, kHumanGetFunctionManager, err_out);
}

inline AbstractRayRuntimeGetInstanceFn resolve_abstract_runtime_get_instance(
    std::string &err_out) {
    static std::atomic<AbstractRayRuntimeGetInstanceFn> slot{nullptr};
    return resolve_cached(slot,
                          kSymAbstractRayRuntimeGetInstance,
                          kHumanAbstractRayRuntimeGetInstance,
                          err_out);
}

inline AbstractRayRuntimeGetGsaFn resolve_abstract_runtime_get_gsa(
    std::string &err_out) {
    static std::atomic<AbstractRayRuntimeGetGsaFn> slot{nullptr};
    return resolve_cached(
        slot, kSymAbstractRayRuntimeGetGsa, kHumanAbstractRayRuntimeGetGsa, err_out);
}

inline GlobalStateAccessorGetInternalKVFn resolve_gsa_get_internal_kv(
    std::string &err_out) {
    static std::atomic<GlobalStateAccessorGetInternalKVFn> slot{nullptr};
    return resolve_cached(slot,
                          kSymGlobalStateAccessorGetInternalKV,
                          kHumanGlobalStateAccessorGetInternalKV,
                          err_out);
}

inline GlobalStateAccessorDtorFn resolve_gsa_dtor(std::string &err_out) {
    static std::atomic<GlobalStateAccessorDtorFn> slot{nullptr};
    return resolve_cached(slot,
                          kSymGlobalStateAccessorDtor,
                          kHumanGlobalStateAccessorDtor,
                          err_out);
}

inline CoreWorkerProcessIsInitializedFn resolve_core_worker_is_initialized(
    std::string &err_out) {
    static std::atomic<CoreWorkerProcessIsInitializedFn> slot{nullptr};
    return resolve_cached(slot,
                          kSymCoreWorkerProcessIsInitialized,
                          kHumanCoreWorkerProcessIsInitialized,
                          err_out);
}

inline CoreWorkerProcessGetCoreWorkerFn resolve_core_worker_get(std::string &err_out) {
    static std::atomic<CoreWorkerProcessGetCoreWorkerFn> slot{nullptr};
    return resolve_cached(slot,
                          kSymCoreWorkerProcessGetCoreWorker,
                          kHumanCoreWorkerProcessGetCoreWorker,
                          err_out);
}

inline CoreWorkerPutFn resolve_core_worker_put(std::string &err_out) {
    static std::atomic<CoreWorkerPutFn> slot{nullptr};
    return resolve_cached(slot, kSymCoreWorkerPut, kHumanCoreWorkerPut, err_out);
}

inline CoreWorkerCancelTaskFn resolve_core_worker_cancel_task(std::string &err_out) {
    static std::atomic<CoreWorkerCancelTaskFn> slot{nullptr};
    return resolve_cached(
        slot, kSymCoreWorkerCancelTask, kHumanCoreWorkerCancelTask, err_out);
}

inline StatusDtorFn resolve_status_dtor(std::string &err_out) {
    static std::atomic<StatusDtorFn> slot{nullptr};
    return resolve_cached(slot, kSymStatusDtor, kHumanStatusDtor, err_out);
}

inline StatusToStringFn resolve_status_to_string(std::string &err_out) {
    static std::atomic<StatusToStringFn> slot{nullptr};
    return resolve_cached(slot, kSymStatusToString, kHumanStatusToString, err_out);
}

/// Empty deleter matching the layout and Itanium `sret` return convention of
/// `std::unique_ptr<ray::gcs::GlobalStateAccessor>` while keeping
/// `GlobalStateAccessor` an incomplete type in this translation unit.
struct __attribute__((visibility("hidden"))) GlobalStateAccessorDeleter {
    void operator()(ray::gcs::GlobalStateAccessor *p) const noexcept {
        if (p != nullptr) {
            std::string err;
            if (auto dtor = resolve_gsa_dtor(err)) {
                dtor(p);
            }
            ::operator delete(static_cast<void *>(p));
        }
    }
};

using OwnedGlobalStateAccessor =
    std::unique_ptr<ray::gcs::GlobalStateAccessor, GlobalStateAccessorDeleter>;

static_assert(sizeof(OwnedGlobalStateAccessor) == sizeof(void *),
              "OwnedGlobalStateAccessor must match std::unique_ptr pointer size");
static_assert(alignof(OwnedGlobalStateAccessor) == alignof(void *),
              "OwnedGlobalStateAccessor must match std::unique_ptr alignment");
static_assert(!std::is_trivially_destructible_v<OwnedGlobalStateAccessor>,
              "OwnedGlobalStateAccessor must use Itanium sret return convention");

using ProcessHelperCreateGsaFn = OwnedGlobalStateAccessor (*)(
    ray::internal::ProcessHelper *, const std::string &, int);

inline ProcessHelperCreateGsaFn resolve_process_helper_create_gsa(
    std::string &err_out) {
    static std::atomic<ProcessHelperCreateGsaFn> slot{nullptr};
    return resolve_cached(
        slot, kSymProcessHelperCreateGsa, kHumanProcessHelperCreateGsa, err_out);
}

inline bool create_temp_global_state_accessor(const std::string &gcs_host,
                                              int gcs_port,
                                              OwnedGlobalStateAccessor &out,
                                              std::string &err_out) {
    auto create_fn = resolve_process_helper_create_gsa(err_out);
    if (create_fn == nullptr) {
        return false;
    }
    if (resolve_gsa_dtor(err_out) == nullptr) {
        return false;
    }
    if (resolve_gsa_get_internal_kv(err_out) == nullptr) {
        return false;
    }
    ray::internal::ProcessHelper helper{};
    out = create_fn(&helper, gcs_host, gcs_port);
    return true;
}

// ─── Deferred #[ctor] registration error channel ─────────────

inline std::mutex &registration_error_mutex() {
    static std::mutex m;
    return m;
}

inline std::string &registration_error_slot() {
    static std::string err;
    return err;
}

inline void record_registration_error(const std::string &err) {
    std::lock_guard<std::mutex> lock(registration_error_mutex());
    if (registration_error_slot().empty()) {
        registration_error_slot() = err;
    }
}

inline std::string get_registration_error() {
    std::lock_guard<std::mutex> lock(registration_error_mutex());
    return registration_error_slot();
}

// ─── Runtime loaded libray_api.so kRayVersion probing ────────

struct LoadedSdkVersionState {
    std::string loaded_path;
    std::string loaded_version;
    std::string parse_error;
};

#ifdef __linux__
inline bool read_elf64_kray_version(const std::string &path,
                                    std::string &version_out,
                                    std::string &err_out) {
    static constexpr const char kTargetSym[] = "_ZL11kRayVersion";
    static constexpr size_t kTargetSymWithNul = sizeof(kTargetSym);  // 17 bytes

    std::ifstream file(path, std::ios::binary | std::ios::ate);
    if (!file.is_open()) {
        err_out = "failed to open loaded SDK library " + path;
        return false;
    }
    const std::streamoff end_pos = file.tellg();
    if (end_pos < static_cast<std::streamoff>(sizeof(Elf64_Ehdr))) {
        err_out = "loaded SDK library " + path + " is smaller than Elf64_Ehdr";
        return false;
    }
    const uint64_t file_size = static_cast<uint64_t>(end_pos);

    Elf64_Ehdr ehdr{};
    file.seekg(0, std::ios::beg);
    if (!file.read(reinterpret_cast<char *>(&ehdr), sizeof(ehdr))) {
        err_out = "failed to read ELF header from " + path;
        return false;
    }
    if (std::memcmp(ehdr.e_ident, ELFMAG, SELFMAG) != 0 ||
        ehdr.e_ident[EI_CLASS] != ELFCLASS64 ||
        ehdr.e_ident[EI_DATA] != ELFDATA2LSB) {
        err_out = "loaded SDK library " + path + " is not a 64-bit little-endian ELF";
        return false;
    }
    if (ehdr.e_shoff == 0 || ehdr.e_shentsize < sizeof(Elf64_Shdr) ||
        ehdr.e_shnum == 0) {
        err_out = "loaded SDK library " + path + " has no ELF section header table";
        return false;
    }

    const uint64_t sh_table_bytes =
        static_cast<uint64_t>(ehdr.e_shentsize) * static_cast<uint64_t>(ehdr.e_shnum);
    if (ehdr.e_shoff > file_size || sh_table_bytes > file_size - ehdr.e_shoff) {
        err_out = "ELF section header table in " + path + " exceeds file size";
        return false;
    }

    std::vector<uint8_t> sh_raw(static_cast<size_t>(sh_table_bytes));
    file.seekg(static_cast<std::streamoff>(ehdr.e_shoff), std::ios::beg);
    if (!file.read(reinterpret_cast<char *>(sh_raw.data()),
                   static_cast<std::streamsize>(sh_raw.size()))) {
        err_out = "failed to read ELF section headers from " + path;
        return false;
    }

    std::vector<Elf64_Shdr> sections(ehdr.e_shnum);
    for (uint16_t i = 0; i < ehdr.e_shnum; ++i) {
        std::memcpy(&sections[i],
                    sh_raw.data() + static_cast<size_t>(i) * ehdr.e_shentsize,
                    sizeof(Elf64_Shdr));
    }

    for (const auto &sec : sections) {
        if (sec.sh_type != SHT_SYMTAB) {
            continue;
        }
        if (sec.sh_entsize < sizeof(Elf64_Sym) || sec.sh_size < sec.sh_entsize) {
            continue;
        }
        if (sec.sh_offset > file_size || sec.sh_size > file_size - sec.sh_offset) {
            err_out = "ELF .symtab section in " + path + " exceeds file size";
            return false;
        }
        if (sec.sh_link >= sections.size()) {
            continue;
        }
        const Elf64_Shdr &str_sec = sections[sec.sh_link];
        if (str_sec.sh_type != SHT_STRTAB || str_sec.sh_size == 0) {
            continue;
        }
        if (str_sec.sh_offset > file_size ||
            str_sec.sh_size > file_size - str_sec.sh_offset) {
            err_out = "ELF .strtab section in " + path + " exceeds file size";
            return false;
        }

        std::vector<uint8_t> symtab(static_cast<size_t>(sec.sh_size));
        file.seekg(static_cast<std::streamoff>(sec.sh_offset), std::ios::beg);
        if (!file.read(reinterpret_cast<char *>(symtab.data()),
                       static_cast<std::streamsize>(symtab.size()))) {
            err_out = "failed to read ELF .symtab from " + path;
            return false;
        }

        std::vector<char> strtab(static_cast<size_t>(str_sec.sh_size));
        file.seekg(static_cast<std::streamoff>(str_sec.sh_offset), std::ios::beg);
        if (!file.read(strtab.data(),
                       static_cast<std::streamsize>(strtab.size()))) {
            err_out = "failed to read ELF .strtab from " + path;
            return false;
        }

        for (uint64_t pos = 0; pos + sec.sh_entsize <= sec.sh_size;
             pos += sec.sh_entsize) {
            Elf64_Sym sym{};
            std::memcpy(&sym, symtab.data() + static_cast<size_t>(pos), sizeof(Elf64_Sym));
            if (sym.st_name >= strtab.size() ||
                strtab.size() - sym.st_name < kTargetSymWithNul) {
                continue;
            }
            if (std::memcmp(strtab.data() + sym.st_name,
                            kTargetSym,
                            kTargetSymWithNul) != 0) {
                continue;
            }
            if (sym.st_shndx == SHN_UNDEF || sym.st_shndx >= sections.size()) {
                err_out = "_ZL11kRayVersion in " + path + " has invalid section index";
                return false;
            }
            const Elf64_Shdr &target_sec = sections[sym.st_shndx];
            if (target_sec.sh_type == SHT_NOBITS || sym.st_value < target_sec.sh_addr) {
                err_out = "_ZL11kRayVersion in " + path + " points outside valid data section";
                return false;
            }
            const uint64_t rel = sym.st_value - target_sec.sh_addr;
            if (rel >= target_sec.sh_size || target_sec.sh_offset > file_size ||
                target_sec.sh_size > file_size - target_sec.sh_offset) {
                err_out = "_ZL11kRayVersion in " + path + " points past section bounds";
                return false;
            }
            const uint64_t str_file_pos = target_sec.sh_offset + rel;
            const size_t read_len = static_cast<size_t>(
                std::min<uint64_t>(target_sec.sh_size - rel, 256));
            char raw_ver[256]{};
            file.seekg(static_cast<std::streamoff>(str_file_pos), std::ios::beg);
            if (!file.read(raw_ver, static_cast<std::streamsize>(read_len))) {
                err_out = "failed to read _ZL11kRayVersion bytes from " + path;
                return false;
            }
            const void *nul_ptr = std::memchr(raw_ver, '\0', read_len);
            if (nul_ptr == nullptr) {
                err_out = "_ZL11kRayVersion in " + path + " lacks a NUL terminator";
                return false;
            }
            const size_t ver_len =
                static_cast<size_t>(static_cast<const char *>(nul_ptr) - raw_ver);
            if (ver_len == 0) {
                err_out = "_ZL11kRayVersion in " + path + " is empty";
                return false;
            }
            for (size_t k = 0; k < ver_len; ++k) {
                const auto ch = static_cast<unsigned char>(raw_ver[k]);
                if (ch < 0x21 || ch > 0x7e) {
                    err_out = "_ZL11kRayVersion in " + path +
                              " contains non-printable ASCII bytes";
                    return false;
                }
            }
            version_out.assign(raw_ver, ver_len);
            return true;
        }
    }

    err_out = "symbol _ZL11kRayVersion was not found in ELF .symtab of " + path;
    return false;
}
#endif

inline const LoadedSdkVersionState &loaded_sdk_version_state() {
    static std::once_flag once;
    static LoadedSdkVersionState state;
    std::call_once(once, []() {
        const auto &lib = sdk_library();
        state.loaded_path = lib.path;
#ifdef __linux__
        if (!lib.has_file_path) {
            state.parse_error = "dladdr did not resolve a filesystem path for libray_api.so";
            return;
        }
        read_elf64_kray_version(lib.path, state.loaded_version, state.parse_error);
#else
        state.parse_error =
            "runtime ELF kRayVersion inspection is only available on Linux";
#endif
    });
    return state;
}

inline bool verify_loaded_sdk_version(std::string &err_out) {
    const auto &state = loaded_sdk_version_state();
    if (!state.loaded_version.empty() &&
        state.loaded_version != RAYRUST_BUILD_RAY_VERSION) {
        err_out = "Ray SDK shared library version mismatch: loaded libray_api.so (" +
                  state.loaded_path + ") is Ray " + state.loaded_version +
                  ", built against Ray C++ SDK " RAYRUST_BUILD_RAY_VERSION;
        return false;
    }
    return true;
}

}  // namespace rayrust_abi

namespace ray {

inline Status::~Status() {
    if (state_ != nullptr) {
        std::string err;
        if (auto fn = rayrust_abi::resolve_status_dtor(err)) {
            fn(this);
        }
        state_ = nullptr;
    }
}

inline std::string Status::ToString() const {
    if (state_ == nullptr) {
        return "OK";
    }
    std::string err;
    if (auto fn = rayrust_abi::resolve_status_to_string(err)) {
        return fn(this);
    }
    return "<non-OK ray::Status (" + err + ")>";
}

}  // namespace ray
