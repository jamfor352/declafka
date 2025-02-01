//! my_kafka_macros/src/lib.rs
//! Defines the #[kafka_listener(...)] attribute.

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse_macro_input, AttributeArgs, ItemFn, Meta, NestedMeta, Lit,
};

/// Usage example:
/// ```ignore
/// #[kafka_listener(
///     topic = "topic-name",
///     config = "my_kafka_lib::get_configuration",
///     deserializer = "my_kafka_lib::string_deserializer"
/// )]
/// fn my_handler(msg: MyStruct) { /* ... */ }
/// ```
///
/// Expands to:
/// - your original `fn my_handler(...) { ... }`
/// - plus `fn my_handler_listener() -> my_kafka_lib::KafkaListener<...>` that
///   constructs a KafkaListener, calling your config + deserializer paths.
#[proc_macro_attribute]
pub fn kafka_listener(attrs: TokenStream, item: TokenStream) -> TokenStream {
    // Parse the user function that the attribute is applied to.
    let input_fn = parse_macro_input!(item as ItemFn);

    // Parse the attribute arguments: e.g. topic="topic-foo", config="json_config"
    let attr_args = parse_macro_input!(attrs as AttributeArgs);

    let mut topic: Option<String> = None;
    let mut config_fn: Option<String> = None;
    let mut deser_fn: Option<String> = None;

    for arg in attr_args {
        if let NestedMeta::Meta(Meta::NameValue(nv)) = arg {
            let name = nv.path.get_ident().map(|id| id.to_string());
            if let Some(name) = name {
                match name.as_str() {
                    "topic" => {
                        if let Lit::Str(lit_str) = nv.lit {
                            topic = Some(lit_str.value());
                        }
                    }
                    "config" => {
                        if let Lit::Str(lit_str) = nv.lit {
                            config_fn = Some(lit_str.value());
                        }
                    }
                    "deserializer" => {
                        if let Lit::Str(lit_str) = nv.lit {
                            deser_fn = Some(lit_str.value());
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    // Extract the function name & argument type
    let fn_name = &input_fn.sig.ident;
    if input_fn.sig.inputs.len() != 1 {
        return syn::Error::new_spanned(
            &input_fn.sig,
            "Expected exactly one argument in Kafka listener function."
        )
            .to_compile_error()
            .into();
    }

    let msg_type = match input_fn.sig.inputs.first().unwrap() {
        syn::FnArg::Typed(pat_type) => &pat_type.ty,
        _ => {
            return syn::Error::new_spanned(
                &input_fn.sig,
                "Unsupported function signature."
            )
                .to_compile_error()
                .into();
        }
    };

    // Provide defaults if not specified (or if parse fails)
    let default_config: syn::Path = syn::parse_quote! { my_kafka_lib::get_configuration };
    let default_deser: syn::Path = syn::parse_quote! { my_kafka_lib::string_deserializer };

    // If user gave "my_kafka_lib::some_config", parse it as a syn::Path. Otherwise use default.
    let config_fn_path = if let Some(cf) = config_fn {
        syn::parse_str::<syn::Path>(&cf).unwrap_or_else(|_| default_config.clone())
    } else {
        default_config.clone()
    };

    let deser_fn_path = if let Some(df) = deser_fn {
        syn::parse_str::<syn::Path>(&df).unwrap_or_else(|_| default_deser.clone())
    } else {
        default_deser.clone()
    };

    let topic_str = topic.unwrap_or_else(|| "<no_topic>".to_owned());

    // The new "factory" function name: e.g. for `fn handle_my_struct`,
    // we generate `fn handle_my_struct_listener()`.
    let factory_fn_name = syn::Ident::new(
        &format!("{}_listener", fn_name),
        fn_name.span()
    );

    // Keep the original function exactly as the user wrote it
    let original_fn = &input_fn;

    // Generate code
    // We no longer prefix the user-supplied deserializer path with `my_kafka_lib::`.
    // Instead, we do:
    //
    //   let cfg = [their config path]();
    //   let deser = [their deserializer path]();
    //
    // Then pass them to `my_kafka_lib::KafkaListener::new(...)`.
    //
    let expanded = quote! {
        // 1) Keep the original function
        #original_fn

        // 2) Generate a function that returns KafkaListener<msg_type>
        #[allow(non_snake_case)]
        pub fn #factory_fn_name() -> my_kafka_lib::KafkaListener<#msg_type> {
            let cfg = #config_fn_path();
            let deser = #deser_fn_path();
            my_kafka_lib::KafkaListener::new(
                #topic_str,
                cfg,
                deser,
                #fn_name,
            )
        }
    };

    expanded.into()
}
