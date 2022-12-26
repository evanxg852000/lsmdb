use criterion::{black_box, criterion_group, criterion_main, Criterion};
use lsmdb_storage::LiteDb;

fn db_set_get(db: &mut LiteDb) {
    db.set(b"name", b"evance").unwrap();
    db.get(b"evance").unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut db = LiteDb::with_default_options("./benches/db").unwrap();
    c.bench_function("db_set_get", |b| b.iter(|| db_set_get(black_box(&mut db))));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
