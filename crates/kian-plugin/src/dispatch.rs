//! Five-mode typed-event dispatch (DESIGN §6.1; mode semantics from
//! the Cordis digest, `the design notes` §3).
//!
//! Like Cordis, the mode is a property of the *dispatch call*:
//! - [`Ctx::emit`] — observe: run [`Ctx::on`] observers in registration
//!   order, no results. (Cordis `emit` does not await; this crate has no
//!   executor, so observers run to completion instead of being spawned —
//!   documented deviation.)
//! - [`Ctx::parallel`] — the same observers, awaited concurrently.
//! - [`Ctx::serial`] — [`Ctx::on_serial`] handlers awaited in registration
//!   order; returns every handler's value in order.
//! - [`Ctx::waterfall`] — a [`WaterfallMiddleware`] chain over a value; any
//!   middleware may short-circuit with [`Flow::Stop`] (design intent for
//!   single-decision events).
//! - [`Ctx::bail`] — [`Ctx::on_bail`] handlers until the first `Some`
//!   ("bail") value is returned.
//!
//! Every registration (`on`, `on_serial`, `on_waterfall`, `on_bail`) returns
//! a [`Disposer`] removing exactly that handler — registrations are
//! reversible side effects.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use futures::future::{BoxFuture, FutureExt, join_all};

use crate::ctx::{Ctx, Disposer};

/// Observer for event `E`: invoked by both `emit` and `parallel`.
pub type Observer<E> = Box<dyn for<'a> Fn(&'a E) -> BoxFuture<'a, ()> + Send + Sync>;

/// Serial handler for event `E` producing a value `R`.
pub type SerialHandler<E, R> = Box<dyn for<'a> Fn(&'a E) -> BoxFuture<'a, R> + Send + Sync>;

/// Bail handler for event `E` producing an optional bail value `T`.
pub type BailHandler<E, T> = Box<dyn for<'a> Fn(&'a E) -> BoxFuture<'a, Option<T>> + Send + Sync>;

/// The result of one waterfall middleware step: continue into the rest of
/// the chain, or short-circuit the whole waterfall (both carry the value).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flow<T> {
    /// Pass the value to the next middleware.
    Next(T),
    /// Short-circuit: later middleware must not run.
    Stop(T),
}

impl<T> Flow<T> {
    pub fn value(self) -> T {
        match self {
            Flow::Next(v) | Flow::Stop(v) => v,
        }
    }

    pub fn is_stop(&self) -> bool {
        matches!(self, Flow::Stop(_))
    }
}

/// The `next()` handle passed to waterfall middleware: invoking `next(value)`
/// runs the rest of the chain (whose result is a [`Flow`] — an inner
/// short-circuit propagates outward). Consumed by [`WaterfallMiddleware`]
/// implementations.
pub struct WaterfallNext<'a, T> {
    run: Box<dyn FnOnce(T) -> BoxFuture<'a, Flow<T>> + Send + 'a>,
}

impl<'a, T> WaterfallNext<'a, T> {
    /// Runs the remainder of the middleware chain with `value`.
    pub async fn next(self, value: T) -> Flow<T> {
        (self.run)(value).await
    }
}

/// One step of a waterfall chain over event `E` and value `T`.
///
/// The `self: Arc<Self>` receiver lets the continuation own the middleware
/// across `await` points without borrowing it, keeping the chain composable
/// for any handler lifetime.
pub trait WaterfallMiddleware<E, T>: Send + Sync + 'static
where
    E: Any + Send + Sync,
    T: Send + 'static,
{
    fn handle<'a>(
        self: Arc<Self>,
        event: &'a E,
        value: T,
        next: WaterfallNext<'a, T>,
    ) -> BoxFuture<'a, Flow<T>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum HandlerKind {
    Observer,
    Serial,
    Waterfall,
    Bail,
}

struct HandlerSlot {
    id: u64,
    erased: Arc<dyn Any + Send + Sync>, // concrete type: Arc<H> for the key's H
}

/// The typed-event handler registry carried by [`Ctx`]. Handlers are keyed
/// by (kind, event/value `TypeId` pairs), so serial/bail/waterfall chains
/// with the same event type but different value types coexist.
#[derive(Default)]
pub(crate) struct Events {
    next_id: AtomicU64,
    handlers: RwLock<HashMap<(HandlerKind, TypeId), Vec<HandlerSlot>>>,
}

impl Events {
    fn insert(&self, key: (HandlerKind, TypeId), erased: Arc<dyn Any + Send + Sync>) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.handlers
            .write()
            .expect("kian-plugin: handlers lock poisoned")
            .entry(key)
            .or_default()
            .push(HandlerSlot { id, erased });
        id
    }

    fn remove(&self, key: (HandlerKind, TypeId), id: u64) {
        let mut handlers = self
            .handlers
            .write()
            .expect("kian-plugin: handlers lock poisoned");
        if let Some(slots) = handlers.get_mut(&key) {
            slots.retain(|slot| slot.id != id);
        }
    }

    /// Clones the typed handlers registered for `key` (registration order).
    fn snapshot<H>(&self, key: (HandlerKind, TypeId)) -> Vec<Arc<H>>
    where
        H: ?Sized + Send + Sync + 'static,
    {
        let handlers = self
            .handlers
            .read()
            .expect("kian-plugin: handlers lock poisoned");
        handlers
            .get(&key)
            .map(|slots| {
                slots
                    .iter()
                    .filter_map(|slot| {
                        Arc::clone(&slot.erased)
                            .downcast::<Arc<H>>()
                            .ok()
                            .map(|wrapped| (*wrapped).clone())
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

impl Ctx {
    fn register_handler<H>(
        &self,
        kind: HandlerKind,
        event_type: TypeId,
        handler: Arc<H>,
    ) -> Disposer
    where
        H: ?Sized + Send + Sync + 'static,
    {
        let key = (kind, event_type);
        let erased: Arc<dyn Any + Send + Sync> = Arc::new(handler);
        let id = self.inner.events.insert(key, erased);
        let inner = Arc::clone(&self.inner);
        Disposer::from_fn(move || inner.events.remove(key, id))
    }

    /// Registers an observer for `E` (used by both `emit` and `parallel`).
    /// The returned [`Disposer`] unregisters exactly this observer.
    pub fn on<E, F>(&self, handler: F) -> Disposer
    where
        E: Any + Send + Sync,
        F: for<'a> Fn(&'a E) -> BoxFuture<'a, ()> + Send + Sync + 'static,
    {
        let boxed: Observer<E> = Box::new(handler);
        self.register_handler(HandlerKind::Observer, TypeId::of::<E>(), Arc::new(boxed))
    }

    /// Registers an ordered handler for `E` producing `R`.
    pub fn on_serial<E, R, F>(&self, handler: F) -> Disposer
    where
        E: Any + Send + Sync,
        R: Send + 'static,
        F: for<'a> Fn(&'a E) -> BoxFuture<'a, R> + Send + Sync + 'static,
    {
        let boxed: SerialHandler<E, R> = Box::new(handler);
        self.register_handler(HandlerKind::Serial, TypeId::of::<(E, R)>(), Arc::new(boxed))
    }

    /// Registers one waterfall middleware step for `(E, T)`.
    pub fn on_waterfall<E, T, M>(&self, middleware: M) -> Disposer
    where
        E: Any + Send + Sync,
        T: Send + 'static,
        M: WaterfallMiddleware<E, T>,
    {
        let boxed: Arc<dyn WaterfallMiddleware<E, T>> = Arc::new(middleware);
        self.register_handler(HandlerKind::Waterfall, TypeId::of::<(E, T)>(), boxed)
    }

    /// Registers a bail handler for `(E, T)`: `Some(value)` bails the
    /// dispatch, `None` defers to the next handler.
    pub fn on_bail<E, T, F>(&self, handler: F) -> Disposer
    where
        E: Any + Send + Sync,
        T: Send + 'static,
        F: for<'a> Fn(&'a E) -> BoxFuture<'a, Option<T>> + Send + Sync + 'static,
    {
        let boxed: BailHandler<E, T> = Box::new(handler);
        self.register_handler(HandlerKind::Bail, TypeId::of::<(E, T)>(), Arc::new(boxed))
    }

    /// Runs all observers of `E` in registration order; no results.
    pub async fn emit<E>(&self, event: &E)
    where
        E: Any + Send + Sync,
    {
        let observers: Vec<Arc<Observer<E>>> = self
            .inner
            .events
            .snapshot((HandlerKind::Observer, TypeId::of::<E>()));
        for observer in observers {
            observer(event).await;
        }
    }

    /// Runs all observers of `E` concurrently and awaits every one.
    pub async fn parallel<E>(&self, event: &E)
    where
        E: Any + Send + Sync,
    {
        let observers: Vec<Arc<Observer<E>>> = self
            .inner
            .events
            .snapshot((HandlerKind::Observer, TypeId::of::<E>()));
        let pending: Vec<BoxFuture<'_, ()>> = observers
            .into_iter()
            .map(|observer| observer(event))
            .collect();
        join_all(pending).await;
    }

    /// Awaits each serial handler of `E` in registration order and returns
    /// every handler's value, in order.
    pub async fn serial<E, R>(&self, event: &E) -> Vec<R>
    where
        E: Any + Send + Sync,
        R: Send + 'static,
    {
        let handlers: Vec<Arc<SerialHandler<E, R>>> = self
            .inner
            .events
            .snapshot((HandlerKind::Serial, TypeId::of::<(E, R)>()));
        let mut results = Vec::with_capacity(handlers.len());
        for handler in handlers {
            results.push(handler(event).await);
        }
        results
    }

    /// Threads `initial` through the waterfall middleware chain for `(E, T)`
    /// in registration order; the first [`Flow::Stop`] short-circuits the
    /// chain (later middleware must not run).
    pub async fn waterfall<E, T>(&self, event: &E, initial: T) -> T
    where
        E: Any + Send + Sync,
        T: Send + 'static,
    {
        let chain: Vec<Arc<dyn WaterfallMiddleware<E, T>>> = self
            .inner
            .events
            .snapshot((HandlerKind::Waterfall, TypeId::of::<(E, T)>()));
        compose_waterfall(&chain, event, 0, initial).await.value()
    }

    /// Runs bail handlers of `(E, T)` in registration order until the first
    /// `Some` value; `None` if every handler deferred.
    pub async fn bail<E, T>(&self, event: &E) -> Option<T>
    where
        E: Any + Send + Sync,
        T: Send + 'static,
    {
        let handlers: Vec<Arc<BailHandler<E, T>>> = self
            .inner
            .events
            .snapshot((HandlerKind::Bail, TypeId::of::<(E, T)>()));
        for handler in handlers {
            if let Some(value) = handler(event).await {
                return Some(value);
            }
        }
        None
    }
}

/// Composes the middleware chain index-inside-out: each step receives a
/// `next` that runs the remainder; a `Flow::Stop` from any level propagates
/// out without invoking deeper middleware.
fn compose_waterfall<'a, E, T>(
    chain: &'a [Arc<dyn WaterfallMiddleware<E, T>>],
    event: &'a E,
    index: usize,
    value: T,
) -> BoxFuture<'a, Flow<T>>
where
    E: Any + Send + Sync,
    T: Send + 'static,
{
    async move {
        if index == chain.len() {
            return Flow::Next(value);
        }
        let middleware = Arc::clone(&chain[index]);
        let next = WaterfallNext {
            run: Box::new(move |v| compose_waterfall(chain, event, index + 1, v)),
        };
        middleware.handle(event, value, next).await
    }
    .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Debug, Default, Clone, PartialEq)]
    struct Ping {
        seq: u32,
    }

    #[tokio::test]
    async fn emit_notifies_all_observers_in_registration_order() {
        let ctx = Ctx::new();
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut keep: Vec<Disposer> = Vec::new();
        for tag in ["a", "b", "c"] {
            let log = Arc::clone(&log);
            keep.push(ctx.on(move |e: &Ping| {
                let log = Arc::clone(&log);
                Box::pin(async move {
                    log.lock().unwrap().push(format!("{tag}:{}", e.seq));
                })
            }));
        }
        ctx.emit(&Ping { seq: 7 }).await;
        assert_eq!(*log.lock().unwrap(), ["a:7", "b:7", "c:7"]);
    }

    #[tokio::test]
    async fn parallel_awaits_all_observers_concurrently() {
        let ctx = Ctx::new();
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let hits = Arc::new(Mutex::new(0u32));
        let mut keep: Vec<Disposer> = Vec::new();
        for _ in 0..2 {
            let barrier = Arc::clone(&barrier);
            let hits = Arc::clone(&hits);
            keep.push(ctx.on(move |_e: &Ping| {
                let barrier = Arc::clone(&barrier);
                let hits = Arc::clone(&hits);
                Box::pin(async move {
                    barrier.wait().await;
                    *hits.lock().unwrap() += 1;
                })
            }));
        }
        // If dispatch were serialized, the first barrier waiter would never
        // release and the timeout would fire.
        let dispatched = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            ctx.parallel(&Ping { seq: 1 }),
        )
        .await;
        assert!(
            dispatched.is_ok(),
            "parallel dispatch must not serialize observers"
        );
        assert_eq!(*hits.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn serial_awaits_in_order_and_returns_every_value() {
        let ctx = Ctx::new();
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut keep: Vec<Disposer> = Vec::new();
        for tag in ["first", "second"] {
            let log = Arc::clone(&log);
            keep.push(ctx.on_serial(move |_e: &Ping| {
                let log = Arc::clone(&log);
                Box::pin(async move {
                    log.lock().unwrap().push(tag);
                    format!("done:{tag}")
                })
            }));
        }
        let results = ctx.serial::<Ping, String>(&Ping { seq: 3 }).await;
        assert_eq!(results, vec!["done:first", "done:second"]);
        assert_eq!(*log.lock().unwrap(), ["first", "second"]);
    }

    struct Add(i32);

    impl WaterfallMiddleware<Ping, i32> for Add {
        fn handle<'a>(
            self: Arc<Self>,
            _event: &'a Ping,
            value: i32,
            next: WaterfallNext<'a, i32>,
        ) -> BoxFuture<'a, Flow<i32>> {
            Box::pin(async move { next.next(value + self.0).await })
        }
    }

    /// Short-circuits the chain once the value exceeds its limit.
    struct OverLimit(i32);

    impl WaterfallMiddleware<Ping, i32> for OverLimit {
        fn handle<'a>(
            self: Arc<Self>,
            _event: &'a Ping,
            value: i32,
            next: WaterfallNext<'a, i32>,
        ) -> BoxFuture<'a, Flow<i32>> {
            Box::pin(async move {
                if value > self.0 {
                    return Flow::Stop(value);
                }
                next.next(value).await
            })
        }
    }

    #[tokio::test]
    async fn waterfall_threads_value_through_all_middleware() {
        assert!(!Flow::Next(1).is_stop());
        assert!(Flow::Stop(1).is_stop());

        let ctx = Ctx::new();
        let _w1 = ctx.on_waterfall::<Ping, i32, _>(Add(1));
        let _w2 = ctx.on_waterfall::<Ping, i32, _>(Add(10));
        let result = ctx.waterfall(&Ping::default(), 5).await;
        assert_eq!(
            result, 16,
            "5 + 1 through the first, + 10 through the second"
        );
    }

    #[tokio::test]
    async fn waterfall_short_circuit_skips_the_rest() {
        let ctx = Ctx::new();
        let seen = Arc::new(Mutex::new(Vec::<i32>::new()));

        struct Record(Arc<Mutex<Vec<i32>>>);
        impl WaterfallMiddleware<Ping, i32> for Record {
            fn handle<'a>(
                self: Arc<Self>,
                _event: &'a Ping,
                value: i32,
                next: WaterfallNext<'a, i32>,
            ) -> BoxFuture<'a, Flow<i32>> {
                Box::pin(async move {
                    self.0.lock().unwrap().push(value);
                    next.next(value).await
                })
            }
        }

        let _w1 = ctx.on_waterfall::<Ping, i32, _>(OverLimit(3));
        let _w2 = ctx.on_waterfall::<Ping, i32, _>(Record(Arc::clone(&seen)));

        let result = ctx.waterfall(&Ping::default(), 5).await;
        assert_eq!(result, 5, "OverLimit(3) stops the chain with the raw value");
        assert!(
            seen.lock().unwrap().is_empty(),
            "middleware after the short-circuit must not run"
        );
    }

    #[tokio::test]
    async fn bail_returns_the_first_some_and_skips_the_rest() {
        let ctx = Ctx::new();
        let third_called = Arc::new(Mutex::new(false));

        let _b1 = ctx
            .on_bail::<Ping, String, _>(move |_e: &Ping| Box::pin(async move { None::<String> }));
        let _b2 = ctx.on_bail::<Ping, String, _>(move |_e: &Ping| {
            Box::pin(async move { Some("veto".to_string()) })
        });
        let flag = Arc::clone(&third_called);
        let _b3 = ctx.on_bail::<Ping, String, _>(move |_e: &Ping| {
            let flag = Arc::clone(&flag);
            Box::pin(async move {
                *flag.lock().unwrap() = true;
                Some("late".to_string())
            })
        });

        let verdict = ctx.bail::<Ping, String>(&Ping::default()).await;
        assert_eq!(verdict.as_deref(), Some("veto"));
        assert!(
            !*third_called.lock().unwrap(),
            "handlers after the first bail value must not run"
        );
    }

    #[tokio::test]
    async fn observer_disposer_unregisters_the_handler() {
        let ctx = Ctx::new();
        let log = Arc::new(Mutex::new(0u32));
        let disposer = {
            let log = Arc::clone(&log);
            ctx.on(move |_e: &Ping| {
                let log = Arc::clone(&log);
                Box::pin(async move {
                    *log.lock().unwrap() += 1;
                })
            })
        };
        disposer.dispose();
        ctx.emit(&Ping::default()).await;
        assert_eq!(*log.lock().unwrap(), 0, "disposed handler must not fire");
    }
}
