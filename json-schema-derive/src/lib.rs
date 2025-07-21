use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Attribute, Data, DataEnum, DataStruct, DeriveInput, Field, Fields, FieldsNamed, FieldsUnnamed,
    Type, Variant, parse_macro_input,
};

#[proc_macro_derive(JsonSchema)]
pub fn derive_json_schema(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = &input.ident;

    let doc = extract_doc_comments(&input.attrs);

    let expanded = match input.data {
        // Newtypes.
        Data::Struct(DataStruct {
            fields:
                Fields::Unnamed(FieldsUnnamed {
                    unnamed: fields, ..
                }),
            ..
        }) => {
            let mut fields = fields.into_iter();
            let inner = fields
                .next()
                .expect("JsonSchema only supports newtype structs with a single field");
            if fields.next().is_some() {
                panic!("JsonSchema only supports newtype structs with a single field");
            }
            let ty = inner.ty;
            let set_description = if let Some(desc) = doc {
                quote! { schema["description"] = serde_json::json!(#desc); }
            } else {
                quote! {}
            };
            quote! {
                impl JsonSchema for #struct_name {
                    fn json_schema() -> serde_json::Value {
                        type T = #ty;
                        let mut schema = T::json_schema();
                        #set_description
                        schema
                    }
                }
            }
        }
        // Regular structs.
        Data::Struct(DataStruct {
            fields: Fields::Named(FieldsNamed { named: fields, .. }),
            ..
        }) => {
            let set_properties = set_properties(fields);
            let set_description = if let Some(desc) = doc {
                quote! { root.insert("description".to_string(), serde_json::json!(#desc)); }
            } else {
                quote! {}
            };
            quote! {
                impl JsonSchema for #struct_name {
                    fn json_schema() -> serde_json::Value {
                        let mut props = serde_json::Map::new();
                        #set_properties

                        let mut root = serde_json::Map::new();
                        root.insert("type".to_string(), serde_json::json!("object"));
                        root.insert("properties".to_string(), serde_json::to_value(props).unwrap());
                        #set_description
                        serde_json::Value::Object(root)
                    }
                }
            }
        }
        // Enums.
        Data::Enum(DataEnum { variants, .. }) => {
            let set_one_of = set_one_of(variants);
            let set_description = if let Some(desc) = doc {
                quote! { root.insert("description".to_string(), serde_json::json!(#desc)); }
            } else {
                quote! {}
            };
            quote! {
                impl JsonSchema for #struct_name {
                    fn json_schema() -> serde_json::Value {
                        let mut one_of = Vec::new();
                        #set_one_of

                        let mut root = serde_json::Map::new();
                        root.insert("type".to_string(), serde_json::json!("object"));
                        root.insert("oneOf".to_string(), serde_json::to_value(one_of).unwrap());
                        #set_description
                        serde_json::Value::Object(root)
                    }
                }
            }
        }
        _ => panic!("JsonSchema does not support this construct"),
    };

    TokenStream::from(expanded)
}

fn set_properties(fields: impl IntoIterator<Item = Field>) -> proc_macro2::TokenStream {
    let mut set_properties = quote! {};
    for field in fields {
        let name = field.ident.unwrap().to_string();
        let doc = extract_doc_comments(&field.attrs);
        let json_schema = json_schema(doc, field.ty);
        set_properties = quote! {
            #set_properties
            props.insert(
                #name.to_string(),
                #json_schema
            );
        }
    }
    set_properties
}

fn set_one_of(variants: impl IntoIterator<Item = Variant>) -> proc_macro2::TokenStream {
    let mut set_one_of = quote! {};
    for variant in variants {
        let doc = extract_doc_comments(&variant.attrs);
        let Fields::Unnamed(variant_fields) = variant.fields else {
            panic!("JsonSchema only supports newtype enum variants");
        };
        let mut variant_fields = variant_fields.unnamed.into_iter();
        let inner = variant_fields
            .next()
            .expect("JsonSchema only supports newtype enum variants with a single field");
        if variant_fields.next().is_some() {
            panic!("JsonSchema only supports newtype enum variants with a single field");
        }
        let json_schema = json_schema(doc, inner.ty);
        set_one_of = quote! {
            #set_one_of
            {
                one_of.push(#json_schema);
            }
        }
    }
    set_one_of
}

fn json_schema(doc: Option<String>, ty: Type) -> proc_macro2::TokenStream {
    match doc {
        Some(desc) => {
            quote! {
                {
                    type T = #ty;
                    let mut schema = T::json_schema();
                    schema["description"] = serde_json::to_value(#desc).unwrap();
                    schema
                }
            }
        }
        None => {
            quote! {
                {
                    type T = #ty;
                    T::json_schema()
                }
            }
        }
    }
}

fn extract_doc_comments(attrs: &[Attribute]) -> Option<String> {
    let docs: Vec<String> = attrs
        .iter()
        .filter_map(|attr| {
            if attr.path().is_ident("doc") {
                if let Ok(meta_name_value) = attr.meta.clone().require_name_value() {
                    if let syn::Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Str(doc),
                        ..
                    }) = &meta_name_value.value
                    {
                        return Some(doc.value().trim().to_string());
                    }
                }
            }
            None
        })
        .collect();

    if docs.is_empty() {
        None
    } else {
        Some(docs.join(" "))
    }
}
