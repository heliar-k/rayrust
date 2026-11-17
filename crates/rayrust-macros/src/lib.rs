//! Proc macros for `rayrust`.
//!
//! ## `#[rayrust::remote]`
//!
//! Marks a function as a Ray remote task. Generates:
//! - A C-compatible callback that deserializes args, calls the function, serializes result
//! - A `register()` function that registers the callback with Ray's FunctionManager
//! - A `{name}_remote()` sync caller (returns `Result`)
//! - A `{name}_remote_async()` async caller (returns `Result`)
//! - A `#[ctor]` auto-registration that runs when the .so is loaded by the Ray worker
//!
//! 两个入口的参数都是泛型的：值参数按 `Serialize` 序列化，`ObjectRef<T>` 与
//! `&ObjectRef<T>` 按引用传（任务拿到的是那个引用的值）。调用器自己持有一份引用
//! 直到提交的 FFI 调用返回。序列化与提交错误都从返回的 `Result` 回到调用方。
//!
//! ## `#[rayrust::actor]`
//!
//! Marks an `impl` block as a Ray actor. Generates:
//! - A factory callback that calls `new()` and returns a raw pointer
//! - Member function callbacks for each method (excluding `new`)
//! - `#[ctor]` auto-registration of all callbacks
//! - `{Type}_actor_create()` function to create instances (accepts `&ActorOptions`)
//! - `{Type}_{method}()` async caller to call methods
//! - `{Type}_{method}_sync()` sync caller to call methods
//!
//! 方法调用器的参数与 `#[remote]` 相同：值或 `ObjectRef<T>`/`&ObjectRef<T>`，错误
//! 都在 `Result` 里。构造参数只接受值。

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    parse_macro_input, FnArg, ImplItem, ItemFn, ItemImpl, ReturnType, Type,
};

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
    let register_fn_name = format_ident!("{}_register", fn_name);
    let callback_fn_name = format_ident!("__rayrust_callback_{}", fn_name);
    let ctor_name = format_ident!("__RAYRUST_CTOR_{}", fn_name);

    let sig = &input_fn.sig;
    let inputs = &sig.inputs;
    let output = &sig.output;

    // Detect if the function is async
    let is_async = sig.asyncness.is_some();

    // Extract argument names and types
    let arg_data: Vec<(syn::Pat, syn::Type)> = inputs
        .iter()
        .filter_map(|arg| {
            if let FnArg::Typed(pat_type) = arg {
                Some((*pat_type.pat.clone(), *pat_type.ty.clone()))
            } else {
                None
            }
        })
        .collect();

    let arg_names: Vec<_> = arg_data.iter().map(|(pat, _)| quote! { #pat }).collect();

    // Build (name, type, index) triples for deserialization
    let arg_deserialize: Vec<_> = arg_data
        .iter()
        .enumerate()
        .map(|(i, (pat, ty))| {
            quote! {
                let #pat: #ty = ::rayrust::deserialize(&__rayrust_deserialized[#i])
                    .expect(concat!("failed to deserialize arg ", #i, " of ", #fn_name_str));
            }
        })
        .collect();

    // 调用器的参数位置：值参数按 `Serialize`，引用参数（`ObjectRef<T>` 按值或
    // `&ObjectRef<T>` 借用）按引用。每个位置一个泛型参数 __RayrustArg0,
    // __RayrustArg1, ...，约束是对应类型的 `TaskArg`。
    //
    // 调用器的参数一律叫 `__arg0`, `__arg1`, ...，不沿用用户写的模式：用户很可能
    // 起一个叫 `ray` 的名字，解构参数（`(a, b): (i32, i32)`）也不能直接当泛型参数
    // 的类型标注。用户模式只在工人侧的反序列化里出现。
    let arg_generic_bounds: Vec<_> = arg_data
        .iter()
        .enumerate()
        .map(|(i, (_, ty))| {
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
    let arg_pushes: Vec<_> = arg_data
        .iter()
        .enumerate()
        .map(|(i, (_, ty))| {
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
            ::rayrust::block_on_async(#fn_name(#(#arg_names),*))
        }
    } else {
        quote! {
            #fn_name(#(#arg_names),*)
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
        ) -> ::rayrust::sys::RayBytes {
            let __rayrust_args_slice = if __rayrust_arg_count == 0 {
                &[][..]
            } else {
                unsafe { ::std::slice::from_raw_parts(__rayrust_args, __rayrust_arg_count) }
            };

            let mut __rayrust_deserialized: Vec<Vec<u8>> =
                Vec::with_capacity(__rayrust_arg_count);
            for __rayrust_i in 0..__rayrust_arg_count {
                let __rayrust_raw = unsafe {
                    ::std::slice::from_raw_parts(
                        __rayrust_args_slice[__rayrust_i].data as *const u8,
                        __rayrust_args_slice[__rayrust_i].len,
                    )
                };
                __rayrust_deserialized.push(__rayrust_raw.to_vec());
            }

            let __rayrust_result = {
                #( #arg_deserialize )*
                #call_expr
            };

            let __rayrust_result_bytes = ::rayrust::serialize(&__rayrust_result)
                .expect(concat!("failed to serialize result of ", #fn_name_str));

            let __rayrust_layout = ::std::alloc::Layout::array::<u8>(
                __rayrust_result_bytes.len())
                .expect("layout alloc failed");
            let __rayrust_ptr = unsafe { ::std::alloc::alloc(__rayrust_layout) };
            if __rayrust_ptr.is_null() {
                ::std::alloc::handle_alloc_error(__rayrust_layout);
            }
            unsafe {
                ::std::ptr::copy_nonoverlapping(
                    __rayrust_result_bytes.as_ptr(),
                    __rayrust_ptr,
                    __rayrust_result_bytes.len(),
                );
            }

            ::rayrust::sys::RayBytes {
                data: __rayrust_ptr as *const ::std::os::raw::c_char,
                len: __rayrust_result_bytes.len(),
            }
        }

        /// Register this function with Ray's FunctionManager.
        pub fn #register_fn_name() {
            let name_c = ::rayrust::sys::to_cstring(#fn_name_str);
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
        /// 都在返回的 `Result` 里。
        pub fn #remote_fn_name #arg_generic_list(
            ray: &::rayrust::Ray,
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
                &::rayrust::TaskOptions::new(),
            )?;
            drop(__rayrust_held);
            Ok(__rayrust_out.cast())
        }

        /// Submit this function as a remote task (async).
        ///
        /// 参数与同步入口相同。返回的 future 是 `'static` 的：`&Ray` 只用来提交，
        /// 轮询结果不再借用它；参数引用的保活随 future 一起走，调用方在轮询之前
        /// drop 掉自己的句柄也不会让对象在提交时失去 owner。
        pub fn #remote_async_fn_name #arg_generic_list(
            ray: &::rayrust::Ray,
            #(#caller_inputs),*
        ) -> impl ::std::future::Future<Output = ::std::result::Result<::rayrust::ObjectRef<#return_type>, ::rayrust::RayError>> + Send + 'static {
            let mut __rayrust_args = ::rayrust::TaskArgs::new();
            #(#arg_pushes)*
            let __rayrust_fut = ray.task_call_async_with_args(
                #fn_name_str,
                __rayrust_args,
                &::rayrust::TaskOptions::new(),
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
    format!("__rayrust_member_{}_{}", type_name.to_lowercase(), method_name)
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
        Type::Path(ref p) => {
            p.path.segments.last()
                .map(|s| s.ident.to_string())
                .expect("actor: could not determine type name")
        }
        _ => panic!("#[rayrust::actor] requires a named type"),
    };

    let type_name_lower = type_name.to_lowercase();
    let type_ident = format_ident!("{}", type_name);
    let factory_str = factory_name(&type_name);
    let factory_ident = format_ident!("{}", factory_str);
    let register_ident = format_ident!("__register_{}_actor", type_name_lower);
    let ctor_ident = format_ident!("__RAYRUST_CTOR_ACTOR_{}", type_name_upper(&type_name));

    // Find `new` constructor and collect methods
    type MethodInfo = (syn::Ident, Vec<(syn::Pat, syn::Type)>, Type);
    let mut methods: Vec<MethodInfo> = Vec::new();
    let mut new_args: Vec<(syn::Pat, syn::Type)> = Vec::new();
    let mut has_new = false;

    for item in &input.items {
        if let ImplItem::Fn(method) = item {
            let method_name = method.sig.ident.to_string();
            if method_name == "new" {
                has_new = true;
                // Extract args (skip self)
                for arg in method.sig.inputs.iter() {
                    if let FnArg::Typed(pat_type) = arg {
                        new_args.push((*pat_type.pat.clone(), *pat_type.ty.clone()));
                    }
                }
            } else {
                // All non-new methods become actor methods (regardless of visibility)
                let m_args: Vec<(syn::Pat, syn::Type)> = method.sig.inputs.iter()
                    .filter_map(|arg| {
                        if let FnArg::Typed(pat_type) = arg {
                            Some((*pat_type.pat.clone(), *pat_type.ty.clone()))
                        } else {
                            None // skip `self`
                        }
                    })
                    .collect();

                let ret_type = match &method.sig.output {
                    ReturnType::Default => Type::Verbatim(quote! { () }),
                    ReturnType::Type(_, ty) => (**ty).clone(),
                };

                methods.push((method.sig.ident.clone(), m_args, ret_type));
            }
        }
    }

    if !has_new {
        panic!("#[rayrust::actor] requires a `new` constructor method");
    }

    // Generate factory callback
    let new_arg_names: Vec<_> = new_args.iter().map(|(p, _)| quote! { #p }).collect();
    let new_arg_deserialize: Vec<_> = new_args.iter()
        .enumerate()
        .map(|(i, (pat, ty))| {
            quote! {
                let #pat: #ty = ::rayrust::deserialize(&__rayrust_deserialized[#i])
                    .expect("failed to deserialize constructor arg");
            }
        })
        .collect();

    // Generate member callbacks
    let member_callbacks: Vec<_> = methods.iter()
        .map(|(method_ident, m_args, _ret_type)| {
            let cb_name = callback_name(&type_name, &method_ident.to_string());
            let cb_ident = format_ident!("{}", cb_name);
            let m_arg_names: Vec<_> = m_args.iter().map(|(p, _)| quote! { #p }).collect();
            let m_arg_deserialize: Vec<_> = m_args.iter()
                .enumerate()
                .map(|(i, (pat, ty))| {
                    quote! {
                        let #pat: #ty = ::rayrust::deserialize(&__rayrust_deserialized[#i])
                            .expect("failed to deserialize method arg");
                    }
                })
                .collect();

            // Check if method takes &mut self or &self
            let self_kind = input.items.iter()
                .find_map(|item| {
                    if let ImplItem::Fn(m) = item {
                        if m.sig.ident == *method_ident {
                            let kind = match m.sig.receiver() {
                                Some(syn::Receiver { reference: Some(_), mutability: Some(_), .. }) => "mut",
                                Some(syn::Receiver { reference: Some(_), mutability: None, .. }) => "ref",
                                Some(syn::Receiver { reference: None, .. }) => "owned",
                                None => "static",
                            };
                            return Some(kind);
                        }
                    }
                    None
                }).unwrap_or("ref");

            let call_expr = match self_kind {
                "mut" => quote! { unsafe { (__rayrust_actor_ptr as *mut #type_ident).as_mut().unwrap().#method_ident(#(#m_arg_names),*) } },
                "ref" => quote! { unsafe { (__rayrust_actor_ptr as *const #type_ident).as_ref().unwrap().#method_ident(#(#m_arg_names),*) } },
                _ => quote! { #type_ident::#method_ident(#(#m_arg_names),*) },
            };

            quote! {
                #[no_mangle]
                #[allow(clippy::missing_safety_doc, clippy::not_unsafe_ptr_arg_deref)]
                pub extern "C" fn #cb_ident(
                    __rayrust_actor_ptr: u64,
                    __rayrust_args: *const ::rayrust::sys::RayBytes,
                    __rayrust_arg_count: usize,
                ) -> ::rayrust::sys::RayBytes {
                    let __rayrust_args_slice = if __rayrust_arg_count == 0 {
                        &[][..]
                    } else {
                        unsafe {
                            ::std::slice::from_raw_parts(__rayrust_args, __rayrust_arg_count)
                        }
                    };

                    let mut __rayrust_deserialized: Vec<Vec<u8>> =
                        Vec::with_capacity(__rayrust_arg_count);
                    for __rayrust_i in 0..__rayrust_arg_count {
                        let __rayrust_raw = unsafe {
                            ::std::slice::from_raw_parts(
                                __rayrust_args_slice[__rayrust_i].data as *const u8,
                                __rayrust_args_slice[__rayrust_i].len,
                            )
                        };
                        __rayrust_deserialized.push(__rayrust_raw.to_vec());
                    }

                    let __rayrust_result = {
                        #( #m_arg_deserialize )*
                        #call_expr
                    };

                    let __rayrust_result_bytes = ::rayrust::serialize(&__rayrust_result)
                        .expect("failed to serialize method result");

                    let __rayrust_layout = ::std::alloc::Layout::array::<u8>(
                        __rayrust_result_bytes.len())
                        .expect("layout alloc failed");
                    let __rayrust_ptr = unsafe { ::std::alloc::alloc(__rayrust_layout) };
                    if __rayrust_ptr.is_null() {
                        ::std::alloc::handle_alloc_error(__rayrust_layout);
                    }
                    unsafe {
                        ::std::ptr::copy_nonoverlapping(
                            __rayrust_result_bytes.as_ptr(),
                            __rayrust_ptr,
                            __rayrust_result_bytes.len(),
                        );
                    }

                    ::rayrust::sys::RayBytes {
                        data: __rayrust_ptr as *const ::std::os::raw::c_char,
                        len: __rayrust_result_bytes.len(),
                    }
                }
            }
        })
        .collect();

    // Generate registration
    let reg_names: Vec<_> = methods.iter()
        .map(|(method_ident, _, _)| {
            let name = format!("{}::{}", factory_str, method_ident);
            let cb_name = callback_name(&type_name, &method_ident.to_string());
            let cb_ident = format_ident!("{}", cb_name);
            quote! {
                let name_c = ::rayrust::sys::to_cstring(#name);
                unsafe {
                    ::rayrust::sys::ray_register_member_function(name_c.as_ptr(), #cb_ident);
                }
            }
        })
        .collect();

    // Generate caller functions
    //
    // 调用器的参数一律叫 `__arg0`, `__arg1`, ...，构造参数也是：用户写的模式只在
    // 工厂/成员回调的反序列化里出现。否则用户参数叫 `ray`/`opts` 或写解构模式时
    // 生成的签名会撞名或不合法。
    let create_fn = format_ident!("{}_actor_create", type_name_lower);
    let create_inputs: Vec<_> = new_args
        .iter()
        .enumerate()
        .map(|(i, (_, ty))| {
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

    let method_callers: Vec<_> = methods.iter()
        .map(|(method_ident, m_args, ret_type)| {
            let caller_name = format_ident!("{}_{}", type_name_lower, method_ident);
            let caller_sync_name = format_ident!("{}_{}_sync", type_name_lower, method_ident);
            let method_full_name = format!("{}::{}", factory_str, method_ident);

            // 方法调用器的参数位置：值参数按 `Serialize`，引用参数（`ObjectRef<T>`
            // 按值或 `&ObjectRef<T>` 借用）按引用，参数名同 `#[remote]`。
            let m_generic_bounds: Vec<_> = m_args
                .iter()
                .enumerate()
                .map(|(i, (_, ty))| {
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
                .map(|(i, (_, ty))| {
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
        ) -> ::rayrust::sys::RayBytes {
            let __rayrust_args_slice = if __rayrust_arg_count == 0 {
                &[][..]
            } else {
                unsafe { ::std::slice::from_raw_parts(__rayrust_args, __rayrust_arg_count) }
            };

            let mut __rayrust_deserialized: Vec<Vec<u8>> =
                Vec::with_capacity(__rayrust_arg_count);
            for __rayrust_i in 0..__rayrust_arg_count {
                let __rayrust_raw = unsafe {
                    ::std::slice::from_raw_parts(
                        __rayrust_args_slice[__rayrust_i].data as *const u8,
                        __rayrust_args_slice[__rayrust_i].len,
                    )
                };
                __rayrust_deserialized.push(__rayrust_raw.to_vec());
            }

            let __rayrust_instance = {
                #( #new_arg_deserialize )*
                #type_ident::new(#(#new_arg_names),*)
            };

            let __rayrust_boxed = ::std::boxed::Box::new(__rayrust_instance);
            let __rayrust_ptr_val = ::std::boxed::Box::into_raw(__rayrust_boxed) as u64;

            let __rayrust_result_bytes = ::rayrust::serialize(&__rayrust_ptr_val)
                .expect("failed to serialize actor ptr");

            let __rayrust_layout = ::std::alloc::Layout::array::<u8>(
                __rayrust_result_bytes.len())
                .expect("layout alloc failed");
            let __rayrust_ptr = unsafe { ::std::alloc::alloc(__rayrust_layout) };
            if __rayrust_ptr.is_null() {
                ::std::alloc::handle_alloc_error(__rayrust_layout);
            }
            unsafe {
                ::std::ptr::copy_nonoverlapping(
                    __rayrust_result_bytes.as_ptr(),
                    __rayrust_ptr,
                    __rayrust_result_bytes.len(),
                );
            }

            ::rayrust::sys::RayBytes {
                data: __rayrust_ptr as *const ::std::os::raw::c_char,
                len: __rayrust_result_bytes.len(),
            }
        }

        // Member callbacks
        #( #member_callbacks )*

        // Registration
        pub fn #register_ident() {
            // Register factory
            let name_c = ::rayrust::sys::to_cstring(#factory_str);
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
    s.chars().map(|c| if c.is_ascii_lowercase() { c.to_ascii_uppercase() } else { c }).collect()
}
