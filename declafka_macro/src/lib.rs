use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse_macro_input, ItemFn, Meta, Expr, ExprLit, Lit,
    punctuated::Punctuated, token::Comma,
    parse::{Parse, ParseStream},
    Path, FnArg,
};

/// A small wrapper type to parse a comma-separated list of attributes.
struct MetaList(Punctuated<Meta, Comma>);

impl Parse for MetaList {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let content = Punctuated::<Meta, Comma>::parse_terminated(input)?;
        Ok(MetaList(content))
    }
}

/// Represents the parsed Kafka listener attributes.
struct KafkaListenerArgs {
    topic: String,
    listener_id: String,
    yaml_path: String,
    deser_fn: Path,
    dlq_topic: Option<String>,
    retry_max_attempts: Option<u32>,
    retry_initial_backoff: Option<u64>,
    retry_max_backoff: Option<u64>,
    retry_multiplier: Option<f64>,
    backend: Option<String>,
}

impl KafkaListenerArgs {
    fn from_meta_list(meta_list: MetaList) -> syn::Result<Self> {
        let mut topic = None;
        let mut listener_id = None;
        let mut yaml_path = None;
        let mut deser_fn = None;
        let mut dlq_topic = None;
        let mut retry_max_attempts = None;
        let mut retry_initial_backoff = None;
        let mut retry_max_backoff = None;
        let mut retry_multiplier = None;
        let mut backend = None;

        for meta in meta_list.0 {
            if let Meta::NameValue(nv) = meta {
                let name = nv.path.get_ident()
                    .ok_or_else(|| syn::Error::new_spanned(&nv.path, "Expected identifier"))?
                    .to_string();
                match name.as_str() {
                    "topic" => topic = Some(Self::extract_string_literal(&nv.value)?),
                    "listener_id" => listener_id = Some(Self::extract_string_literal(&nv.value)?),
                    "yaml_path" => yaml_path = Some(Self::extract_string_literal(&nv.value)?),
                    "deserializer" => {
                        let path_str = Self::extract_string_literal(&nv.value)?;
                        deser_fn = Some(syn::parse_str(&path_str)?);
                    }
                    "dlq_topic" => dlq_topic = Some(Self::extract_string_literal(&nv.value)?),
                    "retry_max_attempts" => {
                        retry_max_attempts = Some(Self::extract_literal_number(&nv.value)?)
                    }
                    "retry_initial_backoff" => {
                        retry_initial_backoff = Some(Self::extract_literal_number(&nv.value)?)
                    }
                    "retry_max_backoff" => {
                        retry_max_backoff = Some(Self::extract_literal_number(&nv.value)?)
                    }
                    "retry_multiplier" => {
                        retry_multiplier = Some(Self::extract_literal_float(&nv.value)?)
                    }
                    "backend" => backend = Some(Self::extract_string_literal(&nv.value)?),
                    _ => {
                        return Err(syn::Error::new_spanned(
                            &nv.path,
                            format!("Unknown attribute: {}", name)
                        ));
                    }
                }
            }
        }

        let topic = topic.ok_or_else(|| syn::Error::new(proc_macro2::Span::call_site(), "topic attribute is required"))?;
        if topic.trim().is_empty() {
            return Err(syn::Error::new(proc_macro2::Span::call_site(), "topic cannot be empty"));
        }
        let listener_id = listener_id.ok_or_else(|| syn::Error::new(proc_macro2::Span::call_site(), "listener_id attribute is required"))?;
        if listener_id.trim().is_empty() {
            return Err(syn::Error::new(proc_macro2::Span::call_site(), "listener_id cannot be empty"));
        }
        let yaml_path = yaml_path.ok_or_else(|| syn::Error::new(proc_macro2::Span::call_site(), "yaml_path attribute is required"))?;
        if yaml_path.trim().is_empty() {
            return Err(syn::Error::new(proc_macro2::Span::call_site(), "yaml_path cannot be empty"));
        }
        let default_deser: Path = syn::parse_quote!(declafka_lib::string_deserializer);
        Ok(KafkaListenerArgs {
            topic,
            listener_id,
            yaml_path,
            deser_fn: deser_fn.unwrap_or(default_deser),
            dlq_topic,
            retry_max_attempts,
            retry_initial_backoff,
            retry_max_backoff,
            retry_multiplier,
            backend,
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
        if let Expr::Lit(ExprLit { lit: Lit::Int(lit_int), .. }) = expr {
            lit_int.base10_parse().map_err(|e| syn::Error::new_spanned(expr, e))
        } else {
            Err(syn::Error::new_spanned(expr, "Expected integer literal"))
        }
    }

    fn extract_literal_float(expr: &Expr) -> syn::Result<f64> {
        match expr {
            Expr::Lit(ExprLit { lit: Lit::Float(lit_float), .. }) => {
                lit_float.base10_parse().map_err(|e| syn::Error::new_spanned(expr, e))
            }
            Expr::Lit(ExprLit { lit: Lit::Int(lit_int), .. }) => {
                lit_int.base10_parse::<i64>().map(|n| n as f64).map_err(|e| syn::Error::new_spanned(expr, e))
            }
            _ => Err(syn::Error::new_spanned(expr, "Expected numeric literal")),
        }
    }
}

#[proc_macro_attribute]
pub fn kafka_listener(attrs: TokenStream, item: TokenStream) -> TokenStream {
    // Parse the input function and attributes.
    let input_fn = parse_macro_input!(item as ItemFn);
    let meta_list = parse_macro_input!(attrs as MetaList);
    let args = match KafkaListenerArgs::from_meta_list(meta_list) {
        Ok(a) => a,
        Err(err) => return err.to_compile_error().into(),
    };

    // Validate the function signature.
    let fn_name = &input_fn.sig.ident;
    if input_fn.sig.inputs.len() != 1 {
        return syn::Error::new_spanned(&input_fn.sig, "Expected exactly one argument in Kafka listener function")
            .to_compile_error().into();
    }
    let msg_type = match input_fn.sig.inputs.first().unwrap() {
        FnArg::Typed(pat_type) => &pat_type.ty,
        _ => return syn::Error::new_spanned(&input_fn.sig, "Unsupported function signature").to_compile_error().into(),
    };

    // Generate the factory function name.
    let factory_fn_name = syn::Ident::new(&format!("{}_listener", fn_name), fn_name.span());
    let topic_str = &args.topic;
    let listener_id = &args.listener_id;
    let yaml_path = &args.yaml_path;
    let deser_fn_path = &args.deser_fn;

    // If retry parameters are specified, chain a call to with_retry_config.
    let retry_config = if args.retry_max_attempts.is_some() ||
        args.retry_initial_backoff.is_some() ||
        args.retry_max_backoff.is_some() ||
        args.retry_multiplier.is_some()
    {
        let max_attempts = args.retry_max_attempts.unwrap_or(3);
        let initial_backoff = args.retry_initial_backoff.unwrap_or(100);
        let max_backoff = args.retry_max_backoff.unwrap_or(10000);
        let multiplier = args.retry_multiplier.unwrap_or(2.0);
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

    // If a dlq_topic is provided, chain a call to with_dead_letter_queue.
    let dlq_setup = if let Some(dlq_topic) = args.dlq_topic {
        quote! {
            .with_dead_letter_queue(#dlq_topic)
        }
    } else {
        quote!()
    };

    // Determine which consumer backend to use.
    let consumer_code = if let Some(backend) = args.backend {
        if backend == "in_memory" {
            quote! { declafka_lib::MockKafkaConsumer::new() }
        } else {
            quote! { declafka_lib::RDKafkaConsumer::new(#yaml_path, #listener_id, #topic_str)? }
        }
    } else {
        quote! { declafka_lib::RDKafkaConsumer::new(#yaml_path, #listener_id, #topic_str)? }
    };

    let expanded = quote! {
        #input_fn

        #[allow(non_snake_case)]
        pub fn #factory_fn_name() -> Result<declafka_lib::KafkaListener<#msg_type, impl declafka_lib::KafkaConsumer>, Box<dyn std::error::Error>> {
            let deser = |payload: &[u8]| #deser_fn_path(payload);
            let listener = declafka_lib::KafkaListener::new(
                #topic_str,
                #listener_id,
                #yaml_path,
                deser,
                #fn_name,
                #consumer_code,
            )?;
            Ok(listener #retry_config #dlq_setup)
        }
    };

    expanded.into()
}