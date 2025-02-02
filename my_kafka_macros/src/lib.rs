//! my_kafka_macros/src/lib.rs
//! Defines the #[kafka_listener(...)] attribute with Syn 2.0+.

use proc_macro::TokenStream;
use quote::quote;
use my_kafka_lib::KafkaListener;
use syn::{
    parse_macro_input, ItemFn, Meta, Expr, ExprLit, Lit,
    punctuated::Punctuated, token::Comma,
    parse::{Parse, ParseStream},
    Path, FnArg,
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

/// A small helper to parse a user-supplied string into a `syn::Path` or fall back to a default.
fn parse_or_default(user_value: &Option<String>, fallback: &Path) -> Path {
    match user_value {
        Some(s) => syn::parse_str::<Path>(s).unwrap_or_else(|_| fallback.clone()),
        None    => fallback.clone(),
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
            // The left side is e.g. `topic`, `config`, etc.
            let name = nv.path.get_ident().map(|id| id.to_string());
            if let Some(name) = name {
                match name.as_str() {
                    "topic" => {
                        // Right side is an Expr, we expect a string literal
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

    // e.g. `fn handle_message(msg: SomeType)`
    let msg_type = match input_fn.sig.inputs.first().unwrap() {
        FnArg::Typed(pat_type) => &pat_type.ty,
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

    // Use our helper to parse or fall back
    let config_fn_path = parse_or_default(&config_fn, &default_config);
    let deser_fn_path  = parse_or_default(&deser_fn, &default_deser);

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
