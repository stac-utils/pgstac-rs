use proc_macro::TokenStream;
use quote::quote;
use syn::ItemFn;
use tokio_postgres::NoTls;

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
            let config = std::env::var("PGSTAC_RS_TEST_DB")
                .unwrap_or("postgresql://username:password@localhost:5432/postgis".to_string());
            let (mut client, connection) = tokio_postgres::connect(&config, NoTls).await.unwrap();
            tokio::spawn(async move {
                connection.await.unwrap()
            });
            let transaction = client.transaction().await.unwrap();
            let client = Client::new(&transaction);
            #ast
            #ident(&client).await;
            transaction.rollback().await.unwrap();
        }
    };
    gen.into()
}
