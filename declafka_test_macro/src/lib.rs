use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse_macro_input, ItemFn, Meta, Lit,
    punctuated::Punctuated, token::Comma,
    parse::{Parse, ParseStream},
};

/// A small wrapper type to parse a comma-separated list of attributes.
struct MetaList(Punctuated<Meta, Comma>);

impl Parse for MetaList {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let content = Punctuated::<Meta, Comma>::parse_terminated(input)?;
        Ok(MetaList(content))
    }
}

/// Represents the parsed Kafka test attributes.
struct KafkaTestArgs {
    topics: Vec<String>,
    port: u16,
    controller_port: u16,
}

impl KafkaTestArgs {
    fn from_meta_list(meta_list: MetaList) -> syn::Result<Self> {
        let mut topics = Vec::new();
        let mut port = 29092;
        let mut controller_port = 29093;

        for meta in meta_list.0 {
            if let Meta::NameValue(nv) = meta {
                let name = nv.path.get_ident()
                    .ok_or_else(|| syn::Error::new_spanned(&nv.path, "Expected identifier"))?
                    .to_string();
                
                match name.as_str() {
                    "topics" => {
                        if let syn::Expr::Lit(expr_lit) = &nv.value {
                            if let Lit::Str(lit_str) = &expr_lit.lit {
                                topics = lit_str.value()
                                    .split(',')
                                    .map(|s| s.trim().to_string())
                                    .collect();
                            }
                        }
                    },
                    "port" => {
                        if let syn::Expr::Lit(expr_lit) = &nv.value {
                            if let Lit::Int(lit_int) = &expr_lit.lit {
                                port = lit_int.base10_parse()?;
                            }
                        }
                    },
                    "controller_port" => {
                        if let syn::Expr::Lit(expr_lit) = &nv.value {
                            if let Lit::Int(lit_int) = &expr_lit.lit {
                                controller_port = lit_int.base10_parse()?;
                            }
                        }
                    },
                    _ => { /* ignore other attributes */ }
                }
            }
        }

        Ok(KafkaTestArgs {
            topics,
            port,
            controller_port,
        })
    }
}

#[proc_macro_attribute]
pub fn kafka_test(attrs: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);
    let meta_list = parse_macro_input!(attrs as MetaList);
    let args = match KafkaTestArgs::from_meta_list(meta_list) {
        Ok(a) => a,
        Err(err) => return err.to_compile_error().into(),
    };

    let fn_name = &input_fn.sig.ident;
    let topics: Vec<String> = args.topics;
    let port = args.port;
    let controller_port = args.controller_port;
    let fn_body = &input_fn.block;

    let topics_tokens = topics.iter().map(|s| quote!(#s));
    let expanded = quote! {
        #[tokio::test]
        async fn #fn_name() {
            use rdkafka::producer::FutureProducer;
            use testcontainers::{
                core::{
                    ExecCommand, 
                    IntoContainerPort, 
                    WaitFor, 
                }, 
                runners::AsyncRunner,
                ContainerAsync, 
                GenericImage, 
                ImageExt
            };
            use log::info;

            env_logger::builder()
                .format_timestamp_millis()
                .filter_level(log::LevelFilter::Info)
                .init();
            let mapped_port = #port;
            let controller_port = #controller_port;
            let topics = [#(#topics_tokens),*];

            info!("Starting Kafka container setup with ports {}, {}", mapped_port, controller_port);

            let container = GenericImage::new("confluentinc/cp-kafka", "7.9.0")
                .with_wait_for(WaitFor::message_on_stdout("Kafka Server started"))
                .with_env_var("KAFKA_NODE_ID", "1")
                .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
                .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", format!("1@127.0.0.1:{}", controller_port))
                .with_env_var("KAFKA_LISTENERS", format!("PLAINTEXT://0.0.0.0:{},CONTROLLER://0.0.0.0:{}", mapped_port, controller_port))
                .with_env_var("KAFKA_ADVERTISED_LISTENERS", format!("PLAINTEXT://127.0.0.1:{},CONTROLLER://127.0.0.1:{}", mapped_port, controller_port))
                .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
                .with_env_var("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
                .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .with_env_var("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qk")
                .with_mapped_port(mapped_port, mapped_port.tcp())
                .with_mapped_port(controller_port, controller_port.tcp())
                .start()
                .await
                .expect("Failed to start Kafka");

            // Create topics
            for topic in topics.iter() {
                info!("Creating topic: {}", topic);
                container
                    .exec(ExecCommand::new([
                        "kafka-topics",
                        "--create",
                        "--topic", topic,
                        "--partitions", "2",
                        "--replication-factor", "1",
                        "--bootstrap-server", &format!("localhost:{}", mapped_port)
                    ]))
                    .await
                    .expect("Failed to create topic");
            }

            let producer: FutureProducer = rdkafka::ClientConfig::new()
                .set("bootstrap.servers", &format!("localhost:{}", mapped_port))
                .set("message.timeout.ms", "5000")
                .set("debug", "all")
                .create()
                .expect("Failed to create producer");

            info!("Kafka setup complete");

            #fn_body
        }
    };

    expanded.into()
}