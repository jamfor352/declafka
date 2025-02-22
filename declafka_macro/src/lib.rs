//! Procedural macro for defining Kafka message handlers
//! 
//! This crate provides the #[kafka_listener] attribute macro which generates
//! boilerplate code for Kafka consumer setup and message handling.

use proc_macro::TokenStream;
use quote::quote;
use declafka_lib::KafkaListener;
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
    dlq_topic: Option<String>,
    retry_max_attempts: Option<u32>,
    retry_initial_backoff: Option<u64>,
    retry_max_backoff: Option<u64>,
    retry_multiplier: Option<f64>,
}

impl KafkaListenerArgs {
    fn from_meta_list(meta_list: MetaList) -> syn::Result<Self> {
        let mut topic: Option<String> = None;
        let mut config_fn: Option<Path> = None;
        let mut deser_fn: Option<Path> = None;
        let mut dlq_topic: Option<String> = None;
        let mut retry_max_attempts: Option<u32> = None;
        let mut retry_initial_backoff: Option<u64> = None;
        let mut retry_max_backoff: Option<u64> = None;
        let mut retry_multiplier: Option<f64> = None;

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
                    "dlq_topic" => {
                        dlq_topic = Some(Self::extract_string_literal(&nv.value)?);
                    }
                    "retry_max_attempts" => {
                        retry_max_attempts = Some(Self::extract_literal_number(&nv.value)?);
                    }
                    "retry_initial_backoff" => {
                        retry_initial_backoff = Some(Self::extract_literal_number(&nv.value)?);
                    }
                    "retry_max_backoff" => {
                        retry_max_backoff = Some(Self::extract_literal_number(&nv.value)?);
                    }
                    "retry_multiplier" => {
                        retry_multiplier = Some(Self::extract_literal_float(&nv.value)?);
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
        let default_config: Path = syn::parse_quote!(declafka_lib::get_configuration);
        let default_deser: Path = syn::parse_quote!(declafka_lib::string_deserializer);

        Ok(KafkaListenerArgs {
            topic,
            config_fn: config_fn.unwrap_or(default_config),
            deser_fn: deser_fn.unwrap_or(default_deser),
            dlq_topic,
            retry_max_attempts,
            retry_initial_backoff,
            retry_max_backoff,
            retry_multiplier,
        })
    }

    fn extract_string_literal(expr: &Expr) -> syn::Result<String> {
        if let Expr::Lit(ExprLit { lit: Lit::Str(lit_str), .. }) = expr {
            Ok(lit_str.value())
        } else {
            Err(syn::Error::new_spanned(expr, "Expected string literal"))
        }
    }

    fn extract_literal_number<T>(expr: &Expr) -> syn::Result<T> 
    where 
        T: std::str::FromStr,
        T::Err: std::fmt::Display,
    {
        match expr {
            Expr::Lit(ExprLit { lit: Lit::Int(lit_int), .. }) => {
                lit_int.base10_parse().map_err(|e| syn::Error::new_spanned(expr, e))
            }
            _ => Err(syn::Error::new_spanned(expr, "Expected integer literal"))
        }
    }

    fn extract_literal_float(expr: &Expr) -> syn::Result<f64> {
        match expr {
            Expr::Lit(ExprLit { lit: Lit::Float(lit_float), .. }) => {
                lit_float.base10_parse().map_err(|e| syn::Error::new_spanned(expr, e))
            }
            Expr::Lit(ExprLit { lit: Lit::Int(lit_int), .. }) => {
                lit_int.base10_parse::<i64>()
                    .map(|n| n as f64)
                    .map_err(|e| syn::Error::new_spanned(expr, e))
            }
            _ => Err(syn::Error::new_spanned(expr, "Expected numeric literal"))
        }
    }
}

/// Usage example:
/// ```ignore
/// #[kafka_listener(
///     topic = "topic-name",
///     config = "declafka_lib::get_configuration",
///     deserializer = "declafka_lib::string_deserializer"
/// )]
/// fn my_handler(msg: MyStruct) { /* ... */ }
/// ```
///
/// Expands to:
/// - your original `fn my_handler(...) { ... }`
/// - plus `fn my_handler_listener() -> declafka_lib::KafkaListener<...>` that
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

    // Generate the retry config and DLQ setup
    let retry_config = if args.retry_max_attempts.is_some() 
        || args.retry_initial_backoff.is_some() 
        || args.retry_max_backoff.is_some() 
        || args.retry_multiplier.is_some() 
    {
        let max_attempts = args.retry_max_attempts
            .map(|v| quote!(#v))
            .unwrap_or(quote!(3));
        let initial_backoff = args.retry_initial_backoff
            .map(|v| quote!(#v))
            .unwrap_or(quote!(100));
        let max_backoff = args.retry_max_backoff
            .map(|v| quote!(#v))
            .unwrap_or(quote!(10000));
        let multiplier = args.retry_multiplier
            .map(|v| quote!(#v))
            .unwrap_or(quote!(2.0));

        quote! {
            .with_retry_config(declafka_lib::RetryConfig {
                max_attempts: #max_attempts,
                initial_backoff_ms: #initial_backoff,
                max_backoff_ms: #max_backoff,
                backoff_multiplier: #multiplier,
            })
        }
    } else {
        quote!()
    };

    let dlq_setup = if let Some(dlq_topic) = args.dlq_topic {
        quote! {
            .with_dead_letter_queue(#dlq_topic)
        }
    } else {
        quote!()
    };

    let expanded = quote! {
        #input_fn

        #[allow(non_snake_case)]
        pub fn #factory_fn_name() -> declafka_lib::KafkaListener<#msg_type> {
            let cfg = #config_fn_path();
            let deser = |payload: &[u8]| #deser_fn_path(payload);
            declafka_lib::KafkaListener::new(
                #topic_str,
                cfg,
                deser,
                #fn_name,
            )
            #retry_config
            #dlq_setup
        }
    };

    expanded.into()
}
