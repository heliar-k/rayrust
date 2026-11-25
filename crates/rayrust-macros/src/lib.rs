//! Proc macros for `rayrust`.
//!
//! ## `#[rayrust::remote]`
//!
//! Marks a function as a Ray remote task. Generates:
//! - A C-compatible callback returning `RayCallbackResult` that unpacks and
//!   deserializes args, runs the function inside `std::panic::catch_unwind`,
//!   and serializes the result (or returns `RAY_CALLBACK_ERROR` on failure/panic)
//! - A `register()` function that registers the callback with Ray's FunctionManager
//! - A `{name}_remote()` sync caller (returns `Result`)
//! - A `{name}_remote_with_options()` sync caller taking `&TaskOptions`
//! - A `{name}_remote_async()` async caller (returns `Result`)
//! - A `{name}_remote_async_with_options()` async caller taking `&TaskOptions`
//! - A `#[ctor]` auto-registration that runs when the .so is loaded by the Ray worker
//!
//! 四个调用器的参数都是泛型的：值参数按 `Serialize` 序列化，`ObjectRef<T>` 与
//! `&ObjectRef<T>` 按引用传（任务拿到的是那个引用的值）。调用器自己持有一份引用
//! 直到提交的 FFI 调用返回。序列化与提交错误都从返回的 `Result` 回到调用方。
//! 带 options 的两个入口把 `&TaskOptions` 放在 `ray` 之后、参数之前；不带
//! options 的两个是它们的薄封装，传 `&TaskOptions::new()`。
//!
//! ## `#[rayrust::actor]`
//!
//! Marks an `impl` block as a Ray actor. Generates:
//! - A factory callback returning `RayCallbackResult` that calls `new()` inside
//!   `catch_unwind` and stores `Box<std::sync::Mutex<T>>` (enforcing `T: Send + 'static`)
//! - Member function callbacks returning `RayCallbackResult` for each method
//!   (excluding `new`; by-value `self` methods are rejected at macro expansion time)
//! - `#[ctor]` auto-registration of all callbacks
//! - `{Type}_actor_create()` function to create instances (accepts `&ActorOptions`)
//! - `{Type}_{method}()` async caller to call methods
//! - `{Type}_{method}_sync()` sync caller to call methods
//!
//! 成员方法回调在获取互斥锁之前完成参数解包与反序列化，因此参数解码失败不会使
//! actor 互斥锁中毒；带 `&self` 或 `&mut self` 接收者的方法通过 `Mutex::lock()`
//! 串行借用状态并在方法返回后、返回值序列化前释放锁；无 `self` 接收者的关联函数
//! 不访问实例指针、不取锁。方法体内发生 `panic!` 时会被 `catch_unwind` 捕获并
//! 通过 `RAY_CALLBACK_ERROR` 传回，同时该实例的互斥锁进入 poisoned 状态，后续
//! 访问该实例状态的调用会返回包含 `"poisoned"` 的错误。
//!
//! 方法调用器的参数与 `#[remote]` 相同：值或 `ObjectRef<T>`/`&ObjectRef<T>`，错误
//! 都在 `Result` 里。构造参数只接受值。

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, FnArg, ImplItem, ItemFn, ItemImpl, ReturnType, Type};

/// 调用器的泛型参数名：名字不可能与用户代码相撞，并且带宏卫生
/// （`mixed_site`），用户自己定义了同名类型也不会被它遮住。
fn generic_arg_ident(index: usize) -> syn::Ident {
    format_ident!(
        "__RayrustArg{}",
        index,
        span = proc_macro2::Span::mixed_site()
    )
}

#[proc_macro_attribute]
pub fn remote(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);

    let fn_name = &input_fn.sig.ident;
    let fn_name_str = fn_name.to_string();

    let remote_fn_name = format_ident!("{}_remote", fn_name);
    let remote_async_fn_name = format_ident!("{}_remote_async", fn_name);
    let remote_fn_name_with_options = format_ident!("{}_remote_with_options", fn_name);
    let remote_async_fn_name_with_options = format_ident!("{}_remote_async_with_options", fn_name);
    let register_fn_name = format_ident!("{}_register", fn_name);
    let callback_fn_name = format_ident!("__rayrust_callback_{}", fn_name);
    let ctor_name = format_ident!("__RAYRUST_CTOR_{}", fn_name);

    let sig = &input_fn.sig;
    let inputs = &sig.inputs;
    let output = &sig.output;

    // Detect if the function is async
    let is_async = sig.asyncness.is_some();

    // Extract argument types (ignore user pattern in call expression via hygienic locals)
    let arg_data: Vec<syn::Type> = inputs
        .iter()
        .filter_map(|arg| {
            if let FnArg::Typed(pat_type) = arg {
                Some(*pat_type.ty.clone())
            } else {
                None
            }
        })
        .collect();

    let expected_arg_count = arg_data.len();

    let cb_arg_idents: Vec<_> = (0..expected_arg_count)
        .map(|i| format_ident!("__rayrust_arg_{}", i))
        .collect();

    let arg_deserialize: Vec<_> = arg_data
        .iter()
        .enumerate()
        .map(|(i, ty)| {
            let local_ident = &cb_arg_idents[i];
            quote! {
                let #local_ident: #ty = ::rayrust::callback::deserialize_arg(
                    __rayrust_raw_args[#i],
                    #i,
                    #fn_name_str,
                )?;
            }
        })
        .collect();

    // 调用器的参数位置：值参数按 `Serialize`，引用参数（`ObjectRef<T>` 按值或
    // `&ObjectRef<T>` 借用）按引用。每个位置一个泛型参数 __RayrustArg0,
    // __RayrustArg1, ...，约束是对应类型的 `TaskArg`。
    //
    // 调用器的参数一律叫 `__arg0`, `__arg1`, ...，不沿用用户写的模式：用户很可能
    // 起一个叫 `ray` 的名字，解构参数（`(a, b): (i32, i32)`）也不能直接当泛型参数
    // 的类型标注。
    let arg_generic_bounds: Vec<_> = arg_data
        .iter()
        .enumerate()
        .map(|(i, ty)| {
            let generic = generic_arg_ident(i);
            quote! { #generic: ::rayrust::TaskArg<#ty> }
        })
        .collect();
    let arg_generic_list = if arg_generic_bounds.is_empty() {
        quote! {}
    } else {
        quote! { <#(#arg_generic_bounds),*> }
    };
    let caller_inputs: Vec<_> = arg_data
        .iter()
        .enumerate()
        .map(|(i, _)| {
            let generic = generic_arg_ident(i);
            let arg_name = format_ident!("__arg{}", i);
            quote! { #arg_name: #generic }
        })
        .collect();
    let arg_idents: Vec<_> = arg_data
        .iter()
        .enumerate()
        .map(|(i, _)| {
            let arg_name = format_ident!("__arg{}", i);
            quote! { #arg_name }
        })
        .collect();
    let arg_pushes: Vec<_> = arg_data
        .iter()
        .enumerate()
        .map(|(i, ty)| {
            let arg_name = format_ident!("__arg{}", i);
            quote! { ::rayrust::TaskArg::<#ty>::push_arg(#arg_name, &mut __rayrust_args); }
        })
        .collect();

    // The return type (strip `async` — the actual return type is the inner type of the Future)
    let return_type: proc_macro2::TokenStream = match output {
        ReturnType::Default => quote! { () },
        ReturnType::Type(_, ty) => quote! { #ty },
    };

    // The call expression: sync vs async
    let call_expr = if is_async {
        quote! {
            // Use the persistent global tokio runtime (created once, reused).
            // This avoids allocating a new runtime per call.
            ::rayrust::block_on_async(#fn_name(#(#cb_arg_idents),*))
        }
    } else {
        quote! {
            #fn_name(#(#cb_arg_idents),*)
        }
    };

    let expanded = quote! {
        // Keep the original function unchanged
        #input_fn

        /// C-compatible callback invoked by Ray's FunctionManager.
        #[no_mangle]
        #[allow(clippy::missing_safety_doc, clippy::not_unsafe_ptr_arg_deref)]
        pub extern "C" fn #callback_fn_name(
            __rayrust_args: *const ::rayrust::sys::RayBytes,
            __rayrust_arg_count: usize,
        ) -> ::rayrust::sys::RayCallbackResult {
            ::rayrust::callback::run_task_callback(#fn_name_str, || {
                let __rayrust_raw_args = unsafe {
                    ::rayrust::callback::unpack_args(
                        __rayrust_args,
                        __rayrust_arg_count,
                        #expected_arg_count,
                        #fn_name_str,
                    )?
                };
                #( #arg_deserialize )*
                ::std::result::Result::Ok(#call_expr)
            })
        }

        /// Register this function with Ray's FunctionManager.
        pub fn #register_fn_name() {
            let name_c = ::rayrust::sys::to_cstring(#fn_name_str)
                .expect("Rust identifier contains no interior NUL byte");
            unsafe {
                ::rayrust::sys::ray_register_function(name_c.as_ptr(), #callback_fn_name);
            }
        }

        /// Auto-registration via #[ctor].
        #[::rayrust::ctor::ctor]
        #[allow(non_snake_case)]
        fn #ctor_name() {
            #register_fn_name();
        }

        /// Submit this function as a remote task (sync).
        ///
        /// 值参数照原样的类型传入；引用参数按 `ObjectRef<T>` 或 `&ObjectRef<T>`
        /// 传入，任务拿到的是那个引用的值，驱动不必先 `get`。调用器自己持有一份
        /// 引用直到提交返回，调用方不需要保证句柄活到某个时刻。序列化与提交错误
        /// 都在返回的 `Result` 里。任务不设任何选项，等价于
        /// [`#remote_fn_name_with_options`] 传 `&TaskOptions::new()`。
        pub fn #remote_fn_name #arg_generic_list(
            ray: &::rayrust::Ray,
            #(#caller_inputs),*
        ) -> ::std::result::Result<::rayrust::ObjectRef<#return_type>, ::rayrust::RayError> {
            #remote_fn_name_with_options(ray, &::rayrust::TaskOptions::new(), #(#arg_idents),*)
        }

        /// [`#remote_fn_name`] 带 [`::rayrust::TaskOptions`] 的版本：任务名、
        /// 资源需求、placement group 绑定与 runtime env 都由 `opts` 给出，
        /// 位置在 `ray` 之后、参数之前。
        pub fn #remote_fn_name_with_options #arg_generic_list(
            ray: &::rayrust::Ray,
            opts: &::rayrust::TaskOptions,
            #(#caller_inputs),*
        ) -> ::std::result::Result<::rayrust::ObjectRef<#return_type>, ::rayrust::RayError> {
            let mut __rayrust_args = ::rayrust::TaskArgs::new();
            #(#arg_pushes)*
            let (__rayrust_data, __rayrust_is_ref, __rayrust_held) =
                __rayrust_args.into_parts()?;
            let __rayrust_refs: ::std::vec::Vec<&[u8]> =
                __rayrust_data.iter().map(|v| v.as_slice()).collect();
            let __rayrust_out = ray.task_call(
                #fn_name_str,
                &__rayrust_refs,
                &__rayrust_is_ref,
                opts,
            )?;
            drop(__rayrust_held);
            Ok(__rayrust_out.cast())
        }

        /// Submit this function as a remote task (async).
        ///
        /// 参数与同步入口相同。返回的 future 是 `'static` 的：`&Ray` 只用来提交，
        /// 轮询结果不再借用它；参数引用的保活随 future 一起走，调用方在轮询之前
        /// drop 掉自己的句柄也不会让对象在提交时失去 owner。任务不设任何选项，
        /// 等价于 [`#remote_async_fn_name_with_options`] 传 `&TaskOptions::new()`。
        pub fn #remote_async_fn_name #arg_generic_list(
            ray: &::rayrust::Ray,
            #(#caller_inputs),*
        ) -> impl ::std::future::Future<Output = ::std::result::Result<::rayrust::ObjectRef<#return_type>, ::rayrust::RayError>> + Send + 'static {
            #remote_async_fn_name_with_options(ray, &::rayrust::TaskOptions::new(), #(#arg_idents),*)
        }

        /// [`#remote_async_fn_name`] 带 [`::rayrust::TaskOptions`] 的版本：`opts`
        /// 在 `ray` 之后、参数之前，语义与同步版相同。
        pub fn #remote_async_fn_name_with_options #arg_generic_list(
            ray: &::rayrust::Ray,
            opts: &::rayrust::TaskOptions,
            #(#caller_inputs),*
        ) -> impl ::std::future::Future<Output = ::std::result::Result<::rayrust::ObjectRef<#return_type>, ::rayrust::RayError>> + Send + 'static {
            let mut __rayrust_args = ::rayrust::TaskArgs::new();
            #(#arg_pushes)*
            let __rayrust_options = opts.to_owned();
            let __rayrust_fut = ray.task_call_async_with_args(
                #fn_name_str,
                __rayrust_args,
                &__rayrust_options,
            );
            async move {
                let __rayrust_out = __rayrust_fut.await?;
                Ok(__rayrust_out.cast())
            }
        }
    };

    expanded.into()
}

// ─── Actor macro ────────────────────────────────────────────────

/// Helper to generate a unique callback function name for an actor method.
fn callback_name(type_name: &str, method_name: &str) -> String {
    format!(
        "__rayrust_member_{}_{}",
        type_name.to_lowercase(),
        method_name
    )
}

/// Helper to generate the factory callback name.
fn factory_name(type_name: &str) -> String {
    format!("__rayrust_actor_factory_{}", type_name.to_lowercase())
}

#[proc_macro_attribute]
pub fn actor(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemImpl);

    // Extract the type name from the impl block
    let type_name = match *input.self_ty {
        Type::Path(ref p) => match p.path.segments.last() {
            Some(s) => s.ident.to_string(),
            None => {
                return syn::Error::new_spanned(
                    &input.self_ty,
                    "#[rayrust::actor] could not determine type name",
                )
                .to_compile_error()
                .into();
            }
        },
        _ => {
            return syn::Error::new_spanned(
                &input.self_ty,
                "#[rayrust::actor] requires a named type",
            )
            .to_compile_error()
            .into();
        }
    };

    let type_name_lower = type_name.to_lowercase();
    let type_ident = format_ident!("{}", type_name);
    let factory_str = factory_name(&type_name);
    let factory_ident = format_ident!("{}", factory_str);
    let register_ident = format_ident!("__register_{}_actor", type_name_lower);
    let ctor_ident = format_ident!("__RAYRUST_CTOR_ACTOR_{}", type_name_upper(&type_name));

    #[derive(Clone, Copy)]
    enum SelfKind {
        Mut,
        Ref,
        Static,
    }

    // Find `new` constructor and collect methods
    type MethodInfo = (syn::Ident, Vec<Type>, Type, SelfKind);
    let mut methods: Vec<MethodInfo> = Vec::new();
    let mut new_args: Vec<Type> = Vec::new();
    let mut has_new = false;

    for item in &input.items {
        if let ImplItem::Fn(method) = item {
            let method_name = method.sig.ident.to_string();
            if method_name == "new" {
                if let Some(receiver) = method.sig.receiver() {
                    return syn::Error::new_spanned(
                        receiver,
                        "#[rayrust::actor] constructor `new` cannot take a `self` receiver",
                    )
                    .to_compile_error()
                    .into();
                }
                has_new = true;
                for arg in method.sig.inputs.iter() {
                    if let FnArg::Typed(pat_type) = arg {
                        new_args.push(*pat_type.ty.clone());
                    }
                }
            } else {
                let self_kind = match method.sig.receiver() {
                    Some(syn::Receiver {
                        reference: Some(_),
                        mutability: Some(_),
                        ..
                    }) => SelfKind::Mut,
                    Some(syn::Receiver {
                        reference: Some(_),
                        mutability: None,
                        ..
                    }) => SelfKind::Ref,
                    Some(
                        receiver @ syn::Receiver {
                            reference: None, ..
                        },
                    ) => {
                        return syn::Error::new_spanned(
                            receiver,
                            "#[rayrust::actor] methods cannot take `self` by value; use `&self`, `&mut self`, or omit `self`",
                        )
                        .to_compile_error()
                        .into();
                    }
                    None => SelfKind::Static,
                };

                // All non-new methods become actor methods (regardless of visibility)
                let m_args: Vec<Type> = method
                    .sig
                    .inputs
                    .iter()
                    .filter_map(|arg| {
                        if let FnArg::Typed(pat_type) = arg {
                            Some(*pat_type.ty.clone())
                        } else {
                            None // skip `self`
                        }
                    })
                    .collect();

                let ret_type = match &method.sig.output {
                    ReturnType::Default => Type::Verbatim(quote! { () }),
                    ReturnType::Type(_, ty) => (**ty).clone(),
                };

                methods.push((method.sig.ident.clone(), m_args, ret_type, self_kind));
            }
        }
    }

    if !has_new {
        return syn::Error::new_spanned(
            &input,
            "#[rayrust::actor] requires a `new` constructor method",
        )
        .to_compile_error()
        .into();
    }

    // Generate factory callback
    let new_expected_count = new_args.len();
    let new_cb_arg_idents: Vec<_> = (0..new_expected_count)
        .map(|i| format_ident!("__rayrust_arg_{}", i))
        .collect();
    let new_arg_deserialize: Vec<_> = new_args
        .iter()
        .enumerate()
        .map(|(i, ty)| {
            let local_ident = &new_cb_arg_idents[i];
            quote! {
                let #local_ident: #ty = ::rayrust::callback::deserialize_arg(
                    __rayrust_raw_args[#i],
                    #i,
                    #factory_str,
                )?;
            }
        })
        .collect();

    // Generate member callbacks
    let member_callbacks: Vec<_> = methods
        .iter()
        .map(|(method_ident, m_args, _ret_type, self_kind)| {
            let cb_name = callback_name(&type_name, &method_ident.to_string());
            let cb_ident = format_ident!("{}", cb_name);
            let method_full_name = format!("{}::{}", factory_str, method_ident);
            let m_expected_count = m_args.len();
            let m_cb_arg_idents: Vec<_> = (0..m_expected_count)
                .map(|i| format_ident!("__rayrust_arg_{}", i))
                .collect();
            let m_arg_deserialize: Vec<_> = m_args
                .iter()
                .enumerate()
                .map(|(i, ty)| {
                    let local_ident = &m_cb_arg_idents[i];
                    quote! {
                        let #local_ident: #ty = ::rayrust::callback::deserialize_arg(
                            __rayrust_raw_args[#i],
                            #i,
                            #method_full_name,
                        )?;
                    }
                })
                .collect();

            let call_expr = match self_kind {
                SelfKind::Mut => quote! {
                    unsafe {
                        ::rayrust::callback::with_actor_mut::<#type_ident, _>(
                            __rayrust_actor_ptr,
                            #method_full_name,
                            |__rayrust_self| __rayrust_self.#method_ident(#(#m_cb_arg_idents),*),
                        )
                    }
                },
                SelfKind::Ref => quote! {
                    unsafe {
                        ::rayrust::callback::with_actor_ref::<#type_ident, _>(
                            __rayrust_actor_ptr,
                            #method_full_name,
                            |__rayrust_self| __rayrust_self.#method_ident(#(#m_cb_arg_idents),*),
                        )
                    }
                },
                SelfKind::Static => quote! {
                    ::std::result::Result::Ok(#type_ident::#method_ident(#(#m_cb_arg_idents),*))
                },
            };

            quote! {
                #[no_mangle]
                #[allow(clippy::missing_safety_doc, clippy::not_unsafe_ptr_arg_deref)]
                pub extern "C" fn #cb_ident(
                    __rayrust_actor_ptr: u64,
                    __rayrust_args: *const ::rayrust::sys::RayBytes,
                    __rayrust_arg_count: usize,
                ) -> ::rayrust::sys::RayCallbackResult {
                    ::rayrust::callback::run_member_callback(#method_full_name, || {
                        let __rayrust_raw_args = unsafe {
                            ::rayrust::callback::unpack_args(
                                __rayrust_args,
                                __rayrust_arg_count,
                                #m_expected_count,
                                #method_full_name,
                            )?
                        };
                        #( #m_arg_deserialize )*
                        #call_expr
                    })
                }
            }
        })
        .collect();

    // Generate registration
    let reg_names: Vec<_> = methods
        .iter()
        .map(|(method_ident, _, _, _)| {
            let name = format!("{}::{}", factory_str, method_ident);
            let cb_name = callback_name(&type_name, &method_ident.to_string());
            let cb_ident = format_ident!("{}", cb_name);
            quote! {
                let name_c = ::rayrust::sys::to_cstring(#name)
                    .expect("Rust identifier contains no interior NUL byte");
                unsafe {
                    ::rayrust::sys::ray_register_member_function(name_c.as_ptr(), #cb_ident);
                }
            }
        })
        .collect();

    // Generate caller functions
    //
    // 调用器的参数一律叫 `__arg0`, `__arg1`, ...，构造参数也是：否则用户参数叫
    // `ray`/`opts` 或写解构模式时生成的签名会撞名或不合法。
    let create_fn = format_ident!("{}_actor_create", type_name_lower);
    let create_inputs: Vec<_> = new_args
        .iter()
        .enumerate()
        .map(|(i, ty)| {
            let arg_name = format_ident!("__arg{}", i);
            quote! { #arg_name: #ty }
        })
        .collect();
    let create_arg_names: Vec<_> = new_args
        .iter()
        .enumerate()
        .map(|(i, _)| {
            let arg_name = format_ident!("__arg{}", i);
            quote! { #arg_name }
        })
        .collect();

    let method_callers: Vec<_> = methods
        .iter()
        .map(|(method_ident, m_args, ret_type, _)| {
            let caller_name = format_ident!("{}_{}", type_name_lower, method_ident);
            let caller_sync_name = format_ident!("{}_{}_sync", type_name_lower, method_ident);
            let method_full_name = format!("{}::{}", factory_str, method_ident);

            // 方法调用器的参数位置：值参数按 `Serialize`，引用参数（`ObjectRef<T>`
            // 按值或 `&ObjectRef<T>` 借用）按引用，参数名同 `#[remote]`。
            let m_generic_bounds: Vec<_> = m_args
                .iter()
                .enumerate()
                .map(|(i, ty)| {
                    let generic = generic_arg_ident(i);
                    quote! { #generic: ::rayrust::TaskArg<#ty> }
                })
                .collect();
            let m_generic_list = if m_generic_bounds.is_empty() {
                quote! {}
            } else {
                quote! { <#(#m_generic_bounds),*> }
            };
            let m_caller_inputs: Vec<_> = m_args
                .iter()
                .enumerate()
                .map(|(i, _)| {
                    let generic = generic_arg_ident(i);
                    let arg_name = format_ident!("__arg{}", i);
                    quote! { #arg_name: #generic }
                })
                .collect();
            let m_arg_pushes: Vec<_> = m_args
                .iter()
                .enumerate()
                .map(|(i, ty)| {
                    let arg_name = format_ident!("__arg{}", i);
                    quote! { ::rayrust::TaskArg::<#ty>::push_arg(#arg_name, &mut __rayrust_args); }
                })
                .collect();

            quote! {
                /// Async actor method caller.
                ///
                /// 值参数照原样的类型传入；引用参数按 `ObjectRef<T>` 或
                /// `&ObjectRef<T>` 传入。返回的 future 是 `'static` 的：`&Ray` 只
                /// 用来提交，参数引用的保活随 future 一起走。
                pub fn #caller_name #m_generic_list(
                    ray: &::rayrust::Ray,
                    handle: &::rayrust::ActorHandle,
                    #(#m_caller_inputs),*
                ) -> impl ::std::future::Future<Output = ::std::result::Result<::rayrust::ObjectRef<#ret_type>, ::rayrust::RayError>> + Send + 'static {
                    let mut __rayrust_args = ::rayrust::TaskArgs::new();
                    #(#m_arg_pushes)*
                    let __rayrust_func = #method_full_name.to_string();
                    let __rayrust_fut =
                        ray.actor_call_async_with_args(handle.id(), &__rayrust_func, __rayrust_args);
                    async move {
                        let __rayrust_out = __rayrust_fut.await?;
                        Ok(__rayrust_out.cast())
                    }
                }

                /// Sync actor method caller.
                ///
                /// 调用器自己持有一份引用直到提交返回；序列化与提交错误都在返回的
                /// `Result` 里。
                pub fn #caller_sync_name #m_generic_list(
                    ray: &::rayrust::Ray,
                    handle: &::rayrust::ActorHandle,
                    #(#m_caller_inputs),*
                ) -> ::std::result::Result<::rayrust::ObjectRef<#ret_type>, ::rayrust::RayError> {
                    let mut __rayrust_args = ::rayrust::TaskArgs::new();
                    #(#m_arg_pushes)*
                    let (__rayrust_data, __rayrust_is_ref, __rayrust_held) =
                        __rayrust_args.into_parts()?;
                    let __rayrust_refs: ::std::vec::Vec<&[u8]> =
                        __rayrust_data.iter().map(|v| v.as_slice()).collect();
                    let __rayrust_func = #method_full_name.to_string();
                    let __rayrust_out = ray.actor_call(
                        handle.id(),
                        &__rayrust_func,
                        &__rayrust_refs,
                        &__rayrust_is_ref,
                    )?;
                    drop(__rayrust_held);
                    Ok(__rayrust_out.cast())
                }
            }
        })
        .collect();

    let expanded = quote! {
        // Keep the original impl block unchanged
        #input

        // Factory callback
        #[no_mangle]
        #[allow(clippy::missing_safety_doc, clippy::not_unsafe_ptr_arg_deref)]
        pub extern "C" fn #factory_ident(
            __rayrust_args: *const ::rayrust::sys::RayBytes,
            __rayrust_arg_count: usize,
        ) -> ::rayrust::sys::RayCallbackResult {
            ::rayrust::callback::run_factory_callback::<#type_ident>(#factory_str, || {
                let __rayrust_raw_args = unsafe {
                    ::rayrust::callback::unpack_args(
                        __rayrust_args,
                        __rayrust_arg_count,
                        #new_expected_count,
                        #factory_str,
                    )?
                };
                #( #new_arg_deserialize )*
                ::std::result::Result::Ok(#type_ident::new(#(#new_cb_arg_idents),*))
            })
        }

        // Member callbacks
        #( #member_callbacks )*

        // Registration
        pub fn #register_ident() {
            // Register factory
            let name_c = ::rayrust::sys::to_cstring(#factory_str)
                .expect("Rust identifier contains no interior NUL byte");
            unsafe {
                ::rayrust::sys::ray_register_function(name_c.as_ptr(), #factory_ident);
            }
            // Register member functions
            #( #reg_names )*
        }

        // Auto-registration via #[ctor]
        #[::rayrust::ctor::ctor]
        #[allow(non_snake_case)]
        fn #ctor_ident() {
            #register_ident();
        }

        // Caller: create actor (accepts ActorOptions for full control)
        pub fn #create_fn(
            ray: &::rayrust::Ray,
            opts: &::rayrust::ActorOptions,
            #(#create_inputs),*
        ) -> ::std::result::Result<::rayrust::ActorHandle, ::rayrust::RayError> {
            let __rayrust_data: ::std::vec::Vec<::std::vec::Vec<u8>> = vec![
                #( ::rayrust::serialize(&#create_arg_names)? ),*
            ];
            let __rayrust_refs: ::std::vec::Vec<&[u8]> =
                __rayrust_data.iter().map(|v| v.as_slice()).collect();
            ray.actor_create(#factory_str, &__rayrust_refs, opts)
        }

        // Caller: call methods
        #( #method_callers )*
    };

    expanded.into()
}

fn type_name_upper(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_lowercase() {
                c.to_ascii_uppercase()
            } else {
                c
            }
        })
        .collect()
}
