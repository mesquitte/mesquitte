use std::{collections::BTreeMap, env, thread, time::Duration};

use client::ClusterClient;
use log::{debug, info};
use maplit::{btreemap, btreeset};
use mesquitte_core::cluster::*;
use store::Request;
use tokio::runtime::Runtime;

#[tokio::test(flavor = "multi_thread")]
async fn raft_test() {
    env::set_var("RUST_LOG", "raft_test=trace,mesquitte_core=trace");
    env_logger::init();

    let (_, app1) = new_raft(
        1,
        "127.0.0.1:21001".parse().unwrap(),
        "127.0.0.1:31001".parse().unwrap(),
        "/Volumes/Ramdisk/data/1",
    )
    .await;
    let _h1 = thread::spawn(move || {
        let rt = Runtime::new().unwrap();
        rt.block_on(app1.run());
    });

    let (_, app2) = new_raft(
        2,
        "127.0.0.1:21002".parse().unwrap(),
        "127.0.0.1:31002".parse().unwrap(),
        "/Volumes/Ramdisk/data/2",
    )
    .await;
    let _h2 = thread::spawn(move || {
        let rt = Runtime::new().unwrap();
        rt.block_on(app2.run());
    });

    let (_, app3) = new_raft(
        3,
        "127.0.0.1:21003".parse().unwrap(),
        "127.0.0.1:31003".parse().unwrap(),
        "/Volumes/Ramdisk/data/3",
    )
    .await;
    let _h3 = thread::spawn(move || {
        let rt = Runtime::new().unwrap();
        rt.block_on(app3.run());
    });

    tokio::time::sleep(Duration::from_millis(2_000)).await;

    info!("=== init single node cluster");

    let mut leader = ClusterClient::new(1, "127.0.0.1:21001".to_string()).await;
    debug!("{:?}", leader.init().await);

    info!("=== add-learner 2");
    debug!(
        "{:?}",
        leader
            .add_learner((
                2,
                "127.0.0.1:21002".to_string(),
                "127.0.0.1:31002".to_string(),
            ))
            .await
    );

    info!("=== add-learner 3");
    debug!(
        "{:?}",
        leader
            .add_learner((
                3,
                "127.0.0.1:21003".to_string(),
                "127.0.0.1:31003".to_string(),
            ))
            .await
    );

    info!("=== metrics after add-learner");
    let x = leader.metrics().await.unwrap();
    assert_eq!(
        &vec![btreeset![1]],
        x.membership_config.membership().get_joint_config()
    );

    let nodes_in_cluster = x
        .membership_config
        .nodes()
        .map(|(nid, node)| (*nid, node.clone()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        btreemap! {
            1 => Node{rpc_addr: "127.0.0.1:21001".to_string(), api_addr: "127.0.0.1:31001".to_string()},
            2 => Node{rpc_addr: "127.0.0.1:21002".to_string(), api_addr: "127.0.0.1:31002".to_string()},
            3 => Node{rpc_addr: "127.0.0.1:21003".to_string(), api_addr: "127.0.0.1:31003".to_string()},
        },
        nodes_in_cluster
    );

    info!("=== change-membership to 1,2,3");
    debug!("{:?}", leader.change_membership(&btreeset! {1,2,3}).await);

    info!("=== metrics after change-member");
    let x = leader.metrics().await.unwrap();
    assert_eq!(
        &vec![btreeset![1, 2, 3]],
        x.membership_config.membership().get_joint_config()
    );

    info!("=== write `foo=bar`");
    debug!(
        "{:?}",
        leader
            .write(&Request::Set {
                key: "foo".to_string(),
                value: "bar".to_string(),
            })
            .await
    );

    tokio::time::sleep(Duration::from_millis(500)).await;
    info!("=== read `foo` on node 1");
    let x = leader.read(&("foo".to_string())).await.unwrap().unwrap();
    assert_eq!("bar", x);

    info!("=== read `foo` on node 2");
    let mut client2 = ClusterClient::new(2, "127.0.0.1:21002".to_string()).await;
    let x = client2.read(&("foo".to_string())).await.unwrap().unwrap();
    assert_eq!("bar", x);

    info!("=== read `foo` on node 3");
    let client3 = ClusterClient::new(3, "127.0.0.1:21003".to_string()).await;
    let x = client3.read(&("foo".to_string())).await.unwrap().unwrap();
    assert_eq!("bar", x);

    info!("=== write `foo` on node 2");
    debug!(
        "{:?}",
        client2
            .write(&Request::Set {
                key: "foo".to_string(),
                value: "wow".to_string(),
            })
            .await
    );

    tokio::time::sleep(Duration::from_millis(500)).await;

    info!("=== read `foo` on node 1");
    let x = leader.read(&("foo".to_string())).await.unwrap().unwrap();
    assert_eq!("wow", x);

    info!("=== read `foo` on node 2");
    let x = client2.read(&("foo".to_string())).await.unwrap().unwrap();
    assert_eq!("wow", x);

    info!("=== read `foo` on node 3");
    let x = client3.read(&("foo".to_string())).await.unwrap().unwrap();
    assert_eq!("wow", x);
}
