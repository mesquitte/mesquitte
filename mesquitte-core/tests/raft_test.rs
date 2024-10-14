use std::{
    collections::{BTreeMap, BTreeSet},
    env, thread,
    time::Duration,
};

use log::info;
use maplit::{btreemap, btreeset};
use mesquitte_core::cluster::*;
use store::Request;
use tokio::runtime::Runtime;
use typ::RaftMetrics;

#[tokio::test(flavor = "multi_thread")]
async fn raft_test() {
    env::set_var(
        "RUST_LOG",
        "raft_test=trace,mesquitte_core=trace,openraft=debug",
    );
    env_logger::init();

    let (raft1, app1) = new_raft(
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

    let (raft2, app2) = new_raft(
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

    let (raft3, app3) = new_raft(
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
    {
        let mut nodes = BTreeMap::new();
        nodes.insert(1, Node::new("127.0.0.1:21001", "127.0.0.1:31001"));
        raft1.initialize(nodes).await.unwrap();
    }
    info!("=== write 2 logs");
    {
        let resp = raft1
            .client_write(Request::set("foo1", "bar1"))
            .await
            .unwrap();
        info!("write resp: {:#?}", resp);
        let resp = raft1
            .client_write(Request::set("foo2", "bar2"))
            .await
            .unwrap();
        info!("write resp: {:#?}", resp);
    }
    info!("=== let node-1 take a snapshot");
    {
        raft1.trigger().snapshot().await.unwrap();

        // Wait for a while to let the snapshot get done.
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    info!("=== metrics after building snapshot");
    {
        let metrics = raft1.metrics().borrow().clone();
        info!("node 1 metrics: {:#?}", metrics);
        assert_eq!(Some(3), metrics.snapshot.map(|x| x.index));
        assert_eq!(Some(3), metrics.purged.map(|x| x.index));
    }
    info!("=== add-learner node-2");
    {
        let node = Node::new("127.0.0.1:21002", "127.0.0.1:31002");
        let resp = raft1.add_learner(2, node, true).await.unwrap();
        info!("add-learner node-2 resp: {:#?}", resp);
    }

    // Wait for a while to let the node 2 to receive snapshot replication.
    tokio::time::sleep(Duration::from_millis(500)).await;

    info!("=== metrics of node 2 that received snapshot");
    {
        let metrics = raft2.metrics().borrow().clone();
        info!("node 2 metrics: {:#?}", metrics);
        assert_eq!(Some(3), metrics.snapshot.map(|x| x.index));
        assert_eq!(Some(3), metrics.purged.map(|x| x.index));
    }

    // In this example, the snapshot is just a copy of the state machine.
    let snapshot = raft2.get_snapshot().await.unwrap();
    info!("node 2 received snapshot: {:#?}", snapshot);
    info!("=== change membership");
    {
        let mut members = BTreeSet::new();
        members.insert(1);
        members.insert(2);
        let resp = raft1.change_membership(members, false).await;
        info!("change resp: {:#?}", resp);
    }
    info!("=== write node 2 logs");
    {
        let resp = raft2
            .client_write(Request::set("foo3", "bar3"))
            .await
            .unwrap();
        info!("write resp: {:#?}", resp);
        let resp = raft2
            .client_write(Request::set("foo4", "bar4"))
            .await
            .unwrap();
        info!("write resp: {:#?}", resp);
    }
    // let client = reqwest::Client::new();
    // info!("=== init single node cluster ===");
    // let x: serde_json::Value = client
    //     .post("http://127.0.0.1:31002/init")
    //     .send()
    //     .await
    //     .unwrap()
    //     .json()
    //     .await
    //     .unwrap();
    // info!("init single node response: {:?}", x);

    // let resp = raft2
    //     .client_write(Request::set("foo1", "bar1"))
    //     .await
    //     .unwrap();
    // println!("write resp: {:#?}", resp);
    // let resp = raft1
    //     .client_write(Request::set("foo2", "bar2"))
    //     .await
    //     .unwrap();
    // println!("write resp: {:#?}", resp);

    // info!("=== metrics ===");
    // let x: serde_json::Value = client
    //     .get("http://127.0.0.1:31002/metrics")
    //     .send()
    //     .await
    //     .unwrap()
    //     .json()
    //     .await
    //     .unwrap();
    // info!("metrics after init: {:?}", x);

    // info!("=== add-learner 2 ===");
    // let x: serde_json::Value = client
    //     .post("http://127.0.0.1:31002/learner")
    //     .json(&serde_json::json!({
    //         "node_id": 1,
    //         "rpc_addr": "127.0.0.1:21001",
    //         "api_addr": "127.0.0.1:31001"
    //     }))
    //     .send()
    //     .await
    //     .unwrap()
    //     .json()
    //     .await
    //     .unwrap();
    // info!("add leaner 2 response: {:?}", x);

    // info!("=== add-learner 3 ===");
    // let x: serde_json::Value = client
    //     .post("http://127.0.0.1:31002/learner")
    //     .json(&serde_json::json!({
    //         "node_id": 3,
    //         "rpc_addr": "127.0.0.1:21003",
    //         "api_addr": "127.0.0.1:31003"
    //     }))
    //     .send()
    //     .await
    //     .unwrap()
    //     .json()
    //     .await
    //     .unwrap();
    // info!("add leaner 3 response: {:?}", x);

    // info!("=== metrics ===");
    // let x: RaftMetrics = client
    //     .get("http://127.0.0.1:31002/metrics")
    //     .send()
    //     .await
    //     .unwrap()
    //     .json()
    //     .await
    //     .unwrap();
    // info!("metrics after add leaner: {:?}", x);
    // assert_eq!(
    //     &vec![btreeset![2]],
    //     x.membership_config.membership().get_joint_config()
    // );
    // let nodes_in_cluster = x
    //     .membership_config
    //     .nodes()
    //     .map(|(nid, node)| (*nid, node.clone()))
    //     .collect::<BTreeMap<_, _>>();
    // assert_eq!(
    //     btreemap! {
    //         1 => Node::new("127.0.0.1:21001", "127.0.0.1:31001"),
    //         2 => Node::new("127.0.0.1:21002", "127.0.0.1:31002"),
    //         3 => Node::new("127.0.0.1:21003", "127.0.0.1:31003"),
    //     },
    //     nodes_in_cluster
    // );

    // info!("=== change-membership 1,2,3 ===");
    // let x: serde_json::Value = client
    //     .post("http://127.0.0.1:31002/membership")
    //     .json(&btreeset! {1, 2, 3})
    //     .send()
    //     .await
    //     .unwrap()
    //     .json()
    //     .await
    //     .unwrap();
    // info!("change membership response: {:?}", x);

    // info!("=== metrics ===");
    // let x: RaftMetrics = client
    //     .get("http://127.0.0.1:31001/metrics")
    //     .send()
    //     .await
    //     .unwrap()
    //     .json()
    //     .await
    //     .unwrap();
    // info!("metrics after change membership: {:?}", x);
    // assert_eq!(
    //     &vec![btreeset![1, 2, 3]],
    //     x.membership_config.membership().get_joint_config()
    // );

    // info!("=== write `foo=bar` on node 1");
    // let x: serde_json::Value = client
    //     .post("http://127.0.0.1:31003/write")
    //     .json(&Request::Set {
    //         key: "foo".to_string(),
    //         value: "bar".to_string(),
    //     })
    //     .send()
    //     .await
    //     .unwrap()
    //     .json()
    //     .await
    //     .unwrap();
    // info!("write foo=bar response: {:?}", x);

    // tokio::time::sleep(Duration::from_millis(1_000)).await;

    // info!("=== read `foo` on node 1");
    // let x: serde_json::Value = client
    //     .post("http://127.0.0.1:31001/read")
    //     .json(&serde_json::json!("foo"))
    //     .send()
    //     .await
    //     .unwrap()
    //     .json()
    //     .await
    //     .unwrap();
    // assert_eq!(serde_json::json!("bar"), x);

    // info!("=== read `foo` on node 2");
    // let x: serde_json::Value = client
    //     .post("http://127.0.0.1:31002/read")
    //     .json(&serde_json::json!("foo"))
    //     .send()
    //     .await
    //     .unwrap()
    //     .json()
    //     .await
    //     .unwrap();
    // assert_eq!(serde_json::json!("bar"), x);

    // info!("=== read `foo` on node 3");
    // let x: serde_json::Value = client
    //     .post("http://127.0.0.1:31003/read")
    //     .json(&serde_json::json!("foo"))
    //     .send()
    //     .await
    //     .unwrap()
    //     .json()
    //     .await
    //     .unwrap();
    // assert_eq!(serde_json::json!("bar"), x);

    // info!("=== metrics ===");
    // let x: serde_json::Value = client
    //     .get("http://127.0.0.1:31001/metrics")
    //     .send()
    //     .await
    //     .unwrap()
    //     .json()
    //     .await
    //     .unwrap();
    // info!("metrics before write node 2: {:?}", x);

    // info!("=== write `zig=zag` on node 2");
    // let x: serde_json::Value = client
    //     .post("http://127.0.0.1:31002/write")
    //     .json(&Request::Set {
    //         key: "foo".to_string(),
    //         value: "zag".to_string(),
    //     })
    //     .send()
    //     .await
    //     .unwrap()
    //     .json()
    //     .await
    //     .unwrap();
    // info!("write zig=zag on node 2 response: {:?}", x);

    // tokio::time::sleep(Duration::from_millis(1_000)).await;

    // info!("=== read `zig` on node 1");
    // let x: serde_json::Value = client
    //     .post("http://127.0.0.1:31001/read")
    //     .json(&serde_json::json!("foo"))
    //     .send()
    //     .await
    //     .unwrap()
    //     .json()
    //     .await
    //     .unwrap();
    // assert_eq!(serde_json::json!("zag"), x);

    // info!("=== read `zig` on node 2");
    // let x: serde_json::Value = client
    //     .post("http://127.0.0.1:31002/read")
    //     .json(&serde_json::json!("zig"))
    //     .send()
    //     .await
    //     .unwrap()
    //     .json()
    //     .await
    //     .unwrap();
    // assert_eq!(serde_json::json!("zag"), x);

    // info!("=== read `zig` on node 3");
    // let x: serde_json::Value = client
    //     .post("http://127.0.0.1:31003/read")
    //     .json(&serde_json::json!("zig"))
    //     .send()
    //     .await
    //     .unwrap()
    //     .json()
    //     .await
    //     .unwrap();
    // assert_eq!(serde_json::json!("zag"), x);

    // tokio::time::sleep(Duration::from_millis(5_000)).await;
    // info!("=== change-membership to 3, ");
    // let _x = client.change_membership(&btreeset! {3}).await.unwrap();

    // tokio::time::sleep(Duration::from_millis(8_000)).await;

    // info!("=== metrics after change-membership to {{3}}");
    // let x = client.metrics().await.unwrap();
    // assert_eq!(
    //     &vec![btreeset![3]],
    //     x.membership_config.membership().get_joint_config()
    // );

    // info!("=== write `foo=zoo` to node-3");
    // let _x = client3
    //     .write(&Request::Set {
    //         key: "foo".to_string(),
    //         value: "zoo".to_string(),
    //     })
    //     .await
    //     .unwrap();

    // info!("=== read `foo=zoo` to node-3");
    // let got = client3.read(&"foo".to_string()).await.unwrap();
    // assert_eq!("zoo", got);
}
