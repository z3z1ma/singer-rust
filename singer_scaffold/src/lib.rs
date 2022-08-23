use proc_macro::TokenStream;
use quote::quote;
use syn::{parse, parse_macro_input, ItemStruct};

/// This validates required fields are present in sink
#[proc_macro_attribute]
pub fn base_sink_fields(args: TokenStream, input: TokenStream) -> TokenStream {
    let mut item_struct = parse_macro_input!(input as ItemStruct);
    let _ = parse_macro_input!(args as parse::Nothing);

    for f in ["stream", "config", "counter", "buffer"] {
        if let syn::Fields::Named(ref mut fields) = item_struct.fields {
            // Inject stream
            if !fields.named.iter().any(|field| {
                field
                    .clone()
                    .ident
                    .map_or(false, |name| name.to_string() == f)
            }) {
                panic!("Missing struct field `{}` in Sink", f)
            };
        }
    }

    return quote! {
        #item_struct
    }
    .into();
}
