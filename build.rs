use akula_table_defs::*;
use quote::*;
use std::{env, io::Write};
fn main() {
    let mut f = std::fs::File::create(&format!(
        "{}/tables.rs",
        env::var_os("OUT_DIR").unwrap().into_string().unwrap()
    ))
    .unwrap();

    f.write_all(
        quote! {
            use static_bytes::Bytes as StaticBytes;
            use akula_table_defs::AutoDupSortConfig;
            use once_cell::sync::Lazy;
            use std::collections::HashMap;
            use super::*;
        }
        .to_string()
        .as_bytes(),
    )
    .unwrap();

    let mut all_tables = vec![];
    let mut map_additions = vec![];
    for (t, info) in &*TABLES {
        let mut is_dup_sort = false;
        let table_name = format_ident!("{}", t);
        f.write_all(
            quote! {
                #[derive(Clone, Copy, Debug)]
                pub struct #table_name;

                impl Table for #table_name {
                    fn db_name(&self) -> string::String<StaticBytes> {
                        unsafe { string::String::from_utf8_unchecked(StaticBytes::from_static(#t.as_bytes())) }
                    }
                }

                impl std::fmt::Display for #table_name {
                    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(f, "{}", #t)
                    }
                }
            }
            .to_string()
            .as_bytes(),
        )
        .unwrap();

        if let Some(DupSortConfig { auto }) = info.dup_sort.as_ref() {
            is_dup_sort = true;
            f.write_all(
                quote! {
                    impl DupSort for #table_name {}
                }
                .to_string()
                .as_bytes(),
            )
            .unwrap();

            let info = if let Some(&AutoDupSortConfig { from, to }) = auto.as_ref() {
                quote! { Some(AutoDupSortConfig { from: #from, to: #to }) }
            } else {
                quote! { None }
            };

            map_additions.push(quote! {
                v.insert(#t, #info);
            })
        }
        all_tables.push(quote! { v.insert(#t, #is_dup_sort); });
    }

    f.write_all(
        quote! {
            pub static TABLE_MAP: Lazy<HashMap<&'static str, bool>> = Lazy::new(|| {
                let mut v = HashMap::new();
                #( #all_tables )*
                v
            });

            pub static DUP_SORT_TABLES: Lazy<HashMap<&'static str, Option<AutoDupSortConfig>>> = Lazy::new(|| {
                let mut v = HashMap::new();
                #( #map_additions )*
                v
            });
        }
        .to_string()
        .as_bytes(),
    )
    .unwrap();
}
