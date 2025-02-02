//! my_kafka_macros/src/lib.rs
//! Defines the #[kafka_listener(...)] attribute with Syn 2.0+.

use proc_macro::TokenStream;
use quote::quote;
use my_kafka_lib::KafkaListener;
use syn::{
    parse_macro_input, ItemFn, Meta, Expr, ExprLit, Lit,
    punctuated::Punctuated, token::Comma,
    parse::{Parse, ParseStream},
    Path,
};

/// A small wrapper type that knows how to parse a comma-separated list of `Meta`.
struct MetaList(Punctuated<Meta, Comma>);

/// Implement `Parse` so we can do `parse_macro_input!(attrs as MetaList)`.
impl Parse for MetaList {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        // Parse zero or more `Meta` items separated by commas
        let content = Punctuated::<Meta, Comma>::parse_terminated(input)?;
        Ok(MetaList(content))
    }
}

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
    // Just to ensure `my_kafka_lib` is recognized as used
    let _dummy: Option<KafkaListener<String>> = None;

    // Parse the function to which the attribute is applied
    let input_fn = parse_macro_input!(item as ItemFn);

    // Parse the attribute as a comma-separated list of Meta
    let meta_list = parse_macro_input!(attrs as MetaList);

    let mut topic: Option<String> = None;
    let mut config_fn: Option<String> = None;
    let mut deser_fn: Option<String> = None;

    // Iterate over each Meta
    for meta in meta_list.0 {
        // We expect `Meta::NameValue(...)` for `key = "value"` style
        if let Meta::NameValue(nv) = meta {
            // e.g. key = something, e.g. topic, config, deserializer
            let name = nv.path.get_ident().map(|id| id.to_string());

            if let Some(name) = name {
                match name.as_str() {
                    "topic" => {
                        // In Syn 2.0, the right-hand side is an `Expr`.
                        // We expect a string literal, so let's match on Expr::Lit.
                        if let Expr::Lit(ExprLit { lit: Lit::Str(lit_str), .. }) = &nv.value {
                            topic = Some(lit_str.value());
                        }
                    }
                    "config" => {
                        if let Expr::Lit(ExprLit { lit: Lit::Str(lit_str), .. }) = &nv.value {
                            config_fn = Some(lit_str.value());
                        }
                    }
                    "deserializer" => {
                        if let Expr::Lit(ExprLit { lit: Lit::Str(lit_str), .. }) = &nv.value {
                            deser_fn = Some(lit_str.value());
                        }
                    }
                    _ => {
                        // Unknown key - either ignore or produce an error
                    }
                }
            }
        }
        // If it's `Meta::Path(...)` or `Meta::List(...)`, handle or ignore as you wish
    }

    // Validate the function signature
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

    // Provide defaults if not specified
    let default_config: Path = syn::parse_quote! { my_kafka_lib::get_configuration };
    let default_deser: Path = syn::parse_quote! { my_kafka_lib::string_deserializer };

    let config_fn_path = if let Some(cf) = config_fn {
        syn::parse_str::<Path>(&cf).unwrap_or_else(|_| default_config.clone())
    } else {
        default_config.clone()
    };

    let deser_fn_path = if let Some(df) = deser_fn {
        syn::parse_str::<Path>(&df).unwrap_or_else(|_| default_deser.clone())
    } else {
        default_deser.clone()
    };

    let topic_str = topic.unwrap_or_else(|| "<no_topic>".to_owned());

    // Generate a new "factory" function name: e.g. `handle_json_listener`.
    let factory_fn_name = syn::Ident::new(
        &format!("{}_listener", fn_name),
        fn_name.span()
    );

    // Keep the original user function
    let original_fn = &input_fn;

    // Generate code
    let expanded = quote! {
        // The user’s original function
        #original_fn

        // A factory function returning KafkaListener<msg_type>
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