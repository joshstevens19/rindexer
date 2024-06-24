use crate::helpers::set_thread_no_logging;
//use crate::logger::set_no_op_logger;
use warp::Filter;

pub async fn run(endpoint: String) {
    let html = format!(
        r#"
    <div style="width: 100%; height: 100%;" id='embedded-sandbox'></div>
    <script src="https://embeddable-sandbox.cdn.apollographql.com/_latest/embeddable-sandbox.umd.production.min.js"></script> 
    <script>
      new window.EmbeddedSandbox({{
        target: '#embedded-sandbox',
        initialEndpoint: '{endpoint}',
      }});
    </script>
    "#,
        endpoint = endpoint
    );

    let route = warp::path::end().map(move || warp::reply::html(html.clone()));

    warp::serve(route).run(([127, 0, 0, 1], 3030)).await;
}

pub fn run_in_child_thread(endpoint: &str) -> &str {
    let endpoint = endpoint.to_string();
    tokio::spawn(async move {
        set_thread_no_logging();

        //let _guard = set_no_op_logger();
        run(endpoint).await;
    });

    "http://localhost:3030"
}
