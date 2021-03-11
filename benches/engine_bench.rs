use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use kvs::{KvStore, KvsEngine, SledKvsEngine};
use rand::prelude::*;
use sled::Db;
use std::{
    collections::{BTreeMap, BTreeSet},
    u32,
};
use tempfile::TempDir;

fn write_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("Write group");
    let key_value = gen_random_key_value();

    group.bench_function("kvs_write", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                (KvStore::open(temp_dir.path()).unwrap(), temp_dir)
            },
            |(mut store, _temp_dir)| {
                for (key, value) in key_value.iter() {
                    store.set(key.to_owned(), value.to_owned()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("sled_write", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                SledKvsEngine::new(Db::start_default(temp_dir.path()).unwrap())
            },
            |mut db| {
                for (key, value) in key_value.iter() {
                    db.set(key.to_owned(), value.to_owned()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn read_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("Read group");
    let key_value = gen_random_key_value();

    group.bench_function("kvs_read", |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut store = KvStore::open(temp_dir.path()).unwrap();
        for (key, value) in key_value.iter() {
            store.set(key.to_owned(), value.to_owned()).unwrap();
        }

        b.iter(|| {
            for (key, ..) in key_value.iter() {
                store.get(key.to_owned()).unwrap();
            }
        })
    });

    group.bench_function("sled_read", |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut db = SledKvsEngine::new(Db::start_default(temp_dir.path()).unwrap());
        for (key, value) in key_value.iter() {
            db.set(key.to_owned(), value.to_owned()).unwrap();
        }

        b.iter(|| {
            for (key, ..) in key_value.iter() {
                db.get(key.to_owned()).unwrap();
            }
        })
    });
}

fn gen_random_key_value() -> BTreeMap<String, String> {
    // 生成 100 个不重复的 1 ~ (100000-3) 范围内的随机数
    let mut rng = rand::thread_rng();
    let mut map: BTreeSet<u32> = BTreeSet::new();

    loop {
        let rn = rng.gen_range(1..100000 - 3);
        map.insert(rn);
        if map.len() == 100 {
            break;
        }
    }

    // 使用上一步生成的随机数拼接 100 对 key/value
    let mut key_value: BTreeMap<String, String> = BTreeMap::new();
    for e in map.iter() {
        let mut key = "key".to_string();
        let mut val = "val".to_string();
        for _n in 1..*e {
            key.push_str("0");
            val.push_str("1");
        }
        key_value.insert(key, val);
    }

    key_value
}

criterion_group!(benches, write_bench, read_bench);
criterion_main!(benches);
