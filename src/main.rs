#[macro_use]
extern crate serde_json;

#[macro_use]
extern crate lazy_static;

mod generator;
use core_affinity;
use elasticsearch::{
    http::transport::Transport, http::StatusCode, indices::IndicesCreateParts,
    indices::IndicesExistsParts, BulkOperation, BulkParts, Elasticsearch, Error,
};
use futures::stream::FuturesUnordered;
use generator::init_fake_data;
use std::sync::atomic::{AtomicUsize, Ordering};
use futures::StreamExt;
use std::thread;
use structopt::StructOpt;
use tokio::{self, runtime};

static CALL_COUNT: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, StructOpt)]
#[structopt(
    name = "burst-elastic",
    about = "A simple tool to benchmark elasticsearch."
)]
struct CLIARGUMENTS {
    #[structopt(
        short,
        long,
        default_value = "1",
        help = "number of threads for each core"
    )]
    thread_num: usize,
    #[structopt(short, long, default_value = "1", help = "number of cores")]
    cores_num: usize,

    #[structopt(help = "elasticsearch url")]
    url: String,

    #[structopt(
        short = "s",
        long,
        default_value = "100",
        help = "size of each chunk to post to bulk api of elasticsearch"
    )]
    chunk_size: usize,
}

lazy_static! {
    static ref ARGUMENTS: CLIARGUMENTS = CLIARGUMENTS::from_args();
    static ref ESCLIENT: Elasticsearch = get_client(&ARGUMENTS.url);
}

fn main() {
    let rt = runtime::Runtime::new().unwrap();
    rt.block_on(async {
        create_index_if_not_exists(&ESCLIENT, "test").await.unwrap();
    });
    get_avaliable_cores();
}

async fn create_index_if_not_exists(client: &Elasticsearch, index: &str) -> Result<(), Error> {
    let exists = client
        .indices()
        .exists(IndicesExistsParts::Index(&[index]))
        .send()
        .await?;

    if exists.status_code() == StatusCode::NOT_FOUND {
        let response = client
            .indices()
            .create(IndicesCreateParts::Index(index))
            .body(json!(
                {
                  "mappings": {
                    "properties": {
                      "body": {
                        "type": "text"
                      }
                    }
                  },
                  "settings": {
                    "index.number_of_shards": 3,
                    "index.number_of_replicas": 0,
                  }
                }
            ))
            .send()
            .await?;

        if !response.status_code().is_success() {
            println!("Error while creating index");
        }
    }

    Ok(())
}

fn get_client(url: &str) -> Elasticsearch {
    let mut tmp = url.to_owned();
    if !url.starts_with("http:") {
        tmp = String::from("http://") + url
    }
    let transport = Transport::single_node(&tmp).unwrap();
    Elasticsearch::new(transport)
}

fn get_current_time() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    since_the_epoch.as_millis()
}

fn async_pool(num: usize) {
    runtime::Builder::new_multi_thread()
        .worker_threads(num)
        .enable_io()
        .enable_time()
        .build()
        .and_then(|rt| {
            rt.block_on(async {
                let mut futs = FuturesUnordered::new();
                loop {
                    let fake_data = init_fake_data(ARGUMENTS.chunk_size);
                    let body: Vec<BulkOperation<_>> = fake_data
                        .iter()
                        .map(|p| {
                            let id = p.id().clone().to_string();
                            BulkOperation::index(p.clone()).id(&id).routing(&id).into()
                        })
                        .collect();
                    futs.push(
                        ESCLIENT
                            .bulk(BulkParts::Index("test"))
                            .body(body)
                            .send()
                    );
                    if futs.len() == ARGUMENTS.chunk_size {
                        while let Some(_) = futs.next().await {
                            CALL_COUNT.fetch_add(1, Ordering::SeqCst);
                            if get_current_time() % 10000 == 0 {
                                println!("called {}", CALL_COUNT.load(Ordering::SeqCst));
                            }
                        }
                    }
                }
            });
            Ok(())
        })
        .unwrap();
}

/* List all the cores except the last one */
fn get_avaliable_cores() {
    core_affinity::get_core_ids().and_then(|core_ids| {
        let handlers = core_ids[0..ARGUMENTS.cores_num]
            .into_iter()
            .map(|core_id| {
                let core_id = core_id.clone();
                thread::spawn(move || {
                    core_affinity::set_for_current(core_id);
                    async_pool(ARGUMENTS.thread_num);
                })
            })
            .collect::<Vec<_>>();
        for handler in handlers.into_iter() {
            handler.join().unwrap();
        }
        Some(())
    });
}
