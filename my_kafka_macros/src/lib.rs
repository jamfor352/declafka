//! Procedural macro for defining Kafka message handlers
//! 
//! This crate provides the #[kafka_listener] attribute macro which generates
//! boilerplate code for Kafka consumer setup and message handling.

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
        let content = Punctuated::<Meta, Comma>::parse_terminated(input)?;
        Ok(MetaList(content))
    }
}

/// Represents the parsed kafka listener attributes.
/// Handles validation and provides defaults for optional parameters.
struct KafkaListenerArgs {
    topic: String,
    config_fn: Path,
    deser_fn: Path,
}

impl KafkaListenerArgs {
    fn from_meta_list(meta_list: MetaList) -> syn::Result<Self> {
        let mut topic: Option<String> = None;
        let mut config_fn: Option<Path> = None;
        let mut deser_fn: Option<Path> = None;

        for meta in meta_list.0 {
            if let Meta::NameValue(nv) = meta {
                let name = nv.path.get_ident()
                    .ok_or_else(|| syn::Error::new_spanned(&nv.path, "Expected identifier"))?
                    .to_string();

                match name.as_str() {
                    "topic" => {
                        topic = Some(Self::extract_string_literal(&nv.value)?);
                    }
                    "config" => {
                        let path_str = Self::extract_string_literal(&nv.value)?;
                        config_fn = Some(syn::parse_str(&path_str)?);
                    }
                    "deserializer" => {
                        let path_str = Self::extract_string_literal(&nv.value)?;
                        deser_fn = Some(syn::parse_str(&path_str)?);
                    }
                    _ => {
                        return Err(syn::Error::new_spanned(
                            &nv.path,
                            format!("Unknown attribute: {}", name)
                        ));
                    }
                }
            }
        }

        // Validate topic
        let topic = topic.ok_or_else(|| 
            syn::Error::new(proc_macro2::Span::call_site(), "topic attribute is required"))?;
        if topic.trim().is_empty() {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                "topic cannot be empty"
            ));
        }

        // Use defaults if not specified
        let default_config: Path = syn::parse_quote!(my_kafka_lib::get_configuration);
        let default_deser: Path = syn::parse_quote!(my_kafka_lib::string_deserializer);

        Ok(KafkaListenerArgs {
            topic,
            config_fn: config_fn.unwrap_or(default_config),
            deser_fn: deser_fn.unwrap_or(default_deser),
        })
    }

    fn extract_string_literal(expr: &Expr) -> syn::Result<String> {
        if let Expr::Lit(ExprLit { lit: Lit::Str(lit_str), .. }) = expr {
            Ok(lit_str.value())
        } else {
            Err(syn::Error::new_spanned(expr, "Expected string literal"))
        }
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
    let _dummy: Option<KafkaListener<String>> = None;

    // Parse the function and attributes
    let input_fn = parse_macro_input!(item as ItemFn);
    let meta_list = parse_macro_input!(attrs as MetaList);

    // Parse and validate the attributes
    let args = match KafkaListenerArgs::from_meta_list(meta_list) {
        Ok(args) => args,
        Err(err) => return err.to_compile_error().into(),
    };

    // Validate function signature
    let fn_name = &input_fn.sig.ident;
    if input_fn.sig.inputs.len() != 1 {
        return syn::Error::new_spanned(
            &input_fn.sig,
            "Expected exactly one argument in Kafka listener function"
        ).to_compile_error().into();
    }

    let msg_type = match input_fn.sig.inputs.first().unwrap() {
        FnArg::Typed(pat_type) => &pat_type.ty,
        _ => return syn::Error::new_spanned(
            &input_fn.sig,
            "Unsupported function signature"
        ).to_compile_error().into(),
    };

    // Generate the factory function name
    let factory_fn_name = syn::Ident::new(
        &format!("{}_listener", fn_name),
        fn_name.span()
    );

    // Generate code
    let topic_str = &args.topic;
    let config_fn_path = &args.config_fn;
    let deser_fn_path = &args.deser_fn;

    let expanded = quote! {
        #input_fn

        #[allow(non_snake_case)]
        pub fn #factory_fn_name() -> my_kafka_lib::KafkaListener<#msg_type> {
            let cfg = #config_fn_path();
            let deser = |payload: &[u8]| #deser_fn_path(payload);
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
