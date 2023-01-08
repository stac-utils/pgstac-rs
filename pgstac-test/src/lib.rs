use proc_macro::TokenStream;
use quote::quote;
use syn::ItemFn;

#[proc_macro_attribute]
pub fn pgstac_test(_args: TokenStream, input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();
    impl_pgstac_test(ast)
}

fn impl_pgstac_test(ast: ItemFn) -> TokenStream {
    let ident = &ast.sig.ident;
    let gen = quote! {
        #[tokio::test]
        async fn #ident() {
            let mut client = POOL.get().await.get().await.unwrap();
            let transaction = client.transaction().await.unwrap();
            let client = Client::new(transaction);
            #ast
            #ident(&client).await;
            client.into_inner().rollback().await.unwrap();
        }
    };
    gen.into()
}
