//! `Ctx` typed service registry, `Plugin`, activation, `Disposer`
//! (card C7, DESIGN §6.1, ADR-004).
//!
//! Mapping from Cordis (ADR-004):
//! - **Stable service keys**: a service is registered *by trait-object type*
//!   (`ctx.provide::<dyn ModelProvider>(impl)` / `ctx.resolve::<dyn
//!   ModelProvider>()`); one implementation per key — a second registration
//!   is a named error.
//! - **Reversible registration**: `trait Plugin { fn apply(self, ctx: &mut
//!   Ctx) -> Result<Disposer, PluginError> }` — everything `apply` installs
//!   (services, event handlers) is unwound by the returned [`Disposer`].
//! - **inject → topology**: plugins declare dependencies by name;
//!   [`PluginSet::activate`] runs them in a deterministic topological order
//!   (ties broken by declaration order); cycles are a named error.
//! - **Disposers unwind in reverse order exactly once**, on explicit
//!   `dispose()` or on drop.
//!
//! Static (compile-time) plugins only — [`crate::kian_plugins!`] assembles a
//! [`PluginSet`]; dynamic loading (dylib/wasm) is Phase 3.

use std::any::{Any, TypeId};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, RwLock};

use crate::dispatch::Events;

/// Marker bound for services stored in [`Ctx`]: any `'static` `Send + Sync`
/// type (trait object or concrete) can occupy a service key.
pub trait Service: Any + Send + Sync {}

impl<T: ?Sized + Any + Send + Sync> Service for T {}

/// Named errors for the plugin subsystem (card C7 acceptance criteria).
#[derive(Debug, thiserror::Error)]
pub enum PluginError {
    /// A second registration for an already-occupied stable service key.
    #[error(
        "service key `{key}` is already provided (service keys are stable: one impl per trait)"
    )]
    DuplicateService { key: &'static str },

    /// Two plugins registered under the same name.
    #[error("plugin name `{name}` is registered twice")]
    DuplicatePlugin { name: String },

    /// A plugin declared a dependency that no registered plugin carries.
    #[error("plugin `{plugin}` declares unknown dependency `{dependency}`")]
    UnknownDependency { plugin: String, dependency: String },

    /// The declared dependency graph contains a cycle; `cycle` lists its
    /// members in walk order.
    #[error("dependency cycle detected among plugins: [{}]", cycle.join(" -> "))]
    DependencyCycle { cycle: Vec<String> },

    /// A plugin's `apply` failed; already-activated plugins were unwound in
    /// reverse activation order.
    #[error("plugin `{plugin}` failed to apply: {source}")]
    Apply {
        plugin: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// Reversible side effect (Cordis disposer). Callbacks run in reverse
/// insertion order, exactly once — on explicit [`Disposer::dispose`] or on
/// drop. Registration and disposal are paired.
#[must_use = "a Disposer holds teardown; dropping it runs teardown immediately — dispose explicitly or pass it on"]
pub struct Disposer {
    callbacks: Vec<Box<dyn FnOnce() + Send + Sync>>,
}

impl Disposer {
    pub const fn new() -> Self {
        Self {
            callbacks: Vec::new(),
        }
    }

    /// A disposer from a single teardown callback.
    pub fn from_fn<F: FnOnce() + Send + Sync + 'static>(callback: F) -> Self {
        Self {
            callbacks: vec![Box::new(callback)],
        }
    }

    /// Appends `later`'s callbacks after `self`'s; disposal unwinds in
    /// reverse insertion order, so `later`'s teardown runs first — matching
    /// reverse activation order when later-registered disposers are merged in.
    pub fn extend(mut self, mut later: Disposer) -> Self {
        self.callbacks.extend(std::mem::take(&mut later.callbacks));
        self
    }

    pub fn is_empty(&self) -> bool {
        self.callbacks.is_empty()
    }

    /// Runs all callbacks now, in reverse insertion order, exactly once.
    pub fn dispose(mut self) {
        for callback in self.callbacks.drain(..).rev() {
            callback();
        }
    }
}

impl Default for Disposer {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Disposer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Disposer")
            .field("callbacks", &self.callbacks.len())
            .finish()
    }
}

impl Drop for Disposer {
    fn drop(&mut self) {
        // `dispose()` drains first, so an explicitly disposed Disposer drops
        // empty — callbacks can never run twice.
        for callback in self.callbacks.drain(..).rev() {
            callback();
        }
    }
}

pub(crate) struct Inner {
    pub(crate) services: RwLock<HashMap<TypeId, Slot>>,
    pub(crate) events: Events,
}

pub(crate) struct Slot {
    // Concrete type is `Arc<S>` for the key's `S`: trait-object-to-`dyn Any`
    // coercion needs a sized inner, hence the double `Arc`.
    value: Arc<dyn Any + Send + Sync>,
}

/// Typed service registry (Cordis `ctx` container). Service keys are stable:
/// the key is the [`TypeId`] of the trait-object type (`dyn Greeter`) or of a
/// concrete type; values are `Arc<dyn Any + Send + Sync>` wrappers around
/// `Arc<S>`.
///
/// Also carries the typed-event handler registry (see [`crate::dispatch`]):
/// `ctx.on*` registers handlers, `ctx.emit`/`parallel`/`serial`/`waterfall`/
/// `bail` dispatch them.
pub struct Ctx {
    pub(crate) inner: Arc<Inner>,
}

impl Ctx {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                services: RwLock::new(HashMap::new()),
                events: Events::default(),
            }),
        }
    }

    /// Registers `svc` under the stable service key `S` and returns the
    /// [`Disposer`] that removes exactly this registration.
    pub fn provide<S: ?Sized + Service>(&self, svc: Arc<S>) -> Result<Disposer, PluginError> {
        let key = TypeId::of::<S>();
        // The slot stores `Arc<S>` itself as the concrete type behind
        // `dyn Any` (trait-object-to-`dyn Any` coercion needs a sized
        // value, and `Arc<S>` is sized even for `S: ?Sized`).
        let slot_value: Arc<dyn Any + Send + Sync> = Arc::new(svc);
        {
            let mut services = self
                .inner
                .services
                .write()
                .expect("kian-plugin: services lock poisoned");
            if services.contains_key(&key) {
                return Err(PluginError::DuplicateService {
                    key: std::any::type_name::<S>(),
                });
            }
            services.insert(
                key,
                Slot {
                    value: slot_value.clone(),
                },
            );
        }
        let inner = Arc::clone(&self.inner);
        Ok(Disposer::from_fn(move || {
            let mut services = inner
                .services
                .write()
                .expect("kian-plugin: services lock poisoned");
            // Remove only if the slot is still ours — a re-registered
            // service must survive a stale disposer.
            let ours = services
                .get(&key)
                .is_some_and(|slot| Arc::ptr_eq(&slot.value, &slot_value));
            if ours {
                services.remove(&key);
            }
        }))
    }

    /// Looks the service for key `S` up by trait type; `None` if absent.
    pub fn resolve<S: ?Sized + Service>(&self) -> Option<Arc<S>> {
        let services = self
            .inner
            .services
            .read()
            .expect("kian-plugin: services lock poisoned");
        let slot = services.get(&TypeId::of::<S>())?;
        let erased = Arc::clone(&slot.value);
        drop(services);
        erased
            .downcast::<Arc<S>>()
            .ok()
            .map(|wrapped| (*wrapped).clone())
    }

    /// Whether a service currently occupies the key `S`.
    pub fn contains_service<S: ?Sized + Service>(&self) -> bool {
        self.inner
            .services
            .read()
            .expect("kian-plugin: services lock poisoned")
            .contains_key(&TypeId::of::<S>())
    }
}

impl Default for Ctx {
    fn default() -> Self {
        Self::new()
    }
}

/// A statically-registered capability unit (ADR-004). `apply` installs
/// services and event handlers into [`Ctx`] and returns the [`Disposer`]
/// that unwinds them; dependencies are declared by plugin *name* and
/// resolved by topological activation (the static equivalent of Cordis
/// `inject`).
pub trait Plugin: Send + Sync + 'static {
    /// Unique plugin name (the dependency-graph node).
    fn name(&self) -> String;

    /// Names of plugins that must be activated before this one.
    fn dependencies(&self) -> Vec<String> {
        Vec::new()
    }

    fn apply(self, ctx: &mut Ctx) -> Result<Disposer, PluginError>;
}

type ApplyFn = Box<dyn FnOnce(&mut Ctx) -> Result<Disposer, PluginError> + Send>;

struct PluginEntry {
    name: String,
    dependencies: Vec<String>,
    // `Option` so activation (which consumes the entry's apply closure)
    // can run exactly once even on error paths.
    apply: Option<ApplyFn>,
}

/// The compile-time plugin set assembled by [`crate::kian_plugins!`].
/// Activation order is derived from declared dependencies, not listing
/// order.
pub struct PluginSet {
    entries: Vec<PluginEntry>,
}

impl PluginSet {
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Registers a plugin (name and dependencies are read immediately).
    pub fn register<P: Plugin>(&mut self, plugin: P) {
        let name = plugin.name();
        let dependencies = plugin.dependencies();
        self.entries.push(PluginEntry {
            name,
            dependencies,
            apply: Some(Box::new(move |ctx| plugin.apply(ctx))),
        });
    }

    /// Registered plugin names, in declaration order.
    pub fn names(&self) -> Vec<String> {
        self.entries.iter().map(|e| e.name.clone()).collect()
    }

    /// Activates every plugin in deterministic topological order. On the
    /// first failure, already-activated plugins are unwound in reverse
    /// activation order exactly once and the error is returned. On success,
    /// the returned [`Disposer`] unwinds the whole set in reverse activation
    /// order.
    pub fn activate(mut self, ctx: &mut Ctx) -> Result<Disposer, PluginError> {
        let order = topological_order(&self.entries)?;
        let mut activated: Vec<Disposer> = Vec::with_capacity(order.len());
        for index in order {
            let entry = &mut self.entries[index];
            let apply = entry
                .apply
                .take()
                .expect("a plugin entry is activated exactly once");
            match apply(ctx) {
                Ok(disposer) => activated.push(disposer),
                Err(source) => {
                    // Unwind exactly what was activated, reverse order, once.
                    for disposer in activated.drain(..).rev() {
                        disposer.dispose();
                    }
                    return Err(PluginError::Apply {
                        plugin: entry.name.clone(),
                        source: Box::new(source),
                    });
                }
            }
        }
        Ok(activated
            .into_iter()
            .fold(Disposer::new(), |earlier, later| earlier.extend(later)))
    }
}

impl Default for PluginSet {
    fn default() -> Self {
        Self::new()
    }
}

/// Deterministic topological order over the declared dependency graph:
/// Kahn's algorithm with declaration-index tiebreaking (the smallest
/// original index among ready nodes wins). Rejects duplicate names, unknown
/// dependencies, and cycles (with the cycle's members named).
fn topological_order(entries: &[PluginEntry]) -> Result<Vec<usize>, PluginError> {
    let mut index_by_name: HashMap<&str, usize> = HashMap::new();
    for (index, entry) in entries.iter().enumerate() {
        if index_by_name.insert(entry.name.as_str(), index).is_some() {
            return Err(PluginError::DuplicatePlugin {
                name: entry.name.clone(),
            });
        }
    }

    for entry in entries {
        for dep in &entry.dependencies {
            if !index_by_name.contains_key(dep.as_str()) {
                return Err(PluginError::UnknownDependency {
                    plugin: entry.name.clone(),
                    dependency: dep.clone(),
                });
            }
        }
    }

    let mut unresolved: Vec<usize> = entries.iter().map(|e| e.dependencies.len()).collect();
    let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); entries.len()];
    for (index, entry) in entries.iter().enumerate() {
        for dep in &entry.dependencies {
            dependents[index_by_name[dep.as_str()]].push(index);
        }
    }

    let mut ready: BinaryHeap<Reverse<usize>> = unresolved
        .iter()
        .enumerate()
        .filter(|(_, count)| **count == 0)
        .map(|(index, _)| Reverse(index))
        .collect();
    let mut order = Vec::with_capacity(entries.len());
    while let Some(Reverse(current)) = ready.pop() {
        order.push(current);
        for &dependent in &dependents[current] {
            unresolved[dependent] -= 1;
            if unresolved[dependent] == 0 {
                ready.push(Reverse(dependent));
            }
        }
    }

    if order.len() == entries.len() {
        return Ok(order);
    }

    // Everything not in `order` is stuck behind a cycle. Walk dependencies
    // among the stuck set until a node repeats; the path from the first
    // occurrence of the repeated node is the cycle.
    let processed: HashSet<usize> = order.iter().copied().collect();
    let mut current = (0..entries.len())
        .find(|i| !processed.contains(i))
        .expect("unprocessed nodes exist whenever a cycle exists");
    let mut seen: HashMap<usize, usize> = HashMap::new();
    let mut path: Vec<usize> = Vec::new();
    loop {
        if let Some(position) = seen.get(&current) {
            let cycle = path[*position..]
                .iter()
                .map(|&i| entries[i].name.clone())
                .collect();
            return Err(PluginError::DependencyCycle { cycle });
        }
        seen.insert(current, path.len());
        path.push(current);
        current = entries[current]
            .dependencies
            .iter()
            .map(|dep| index_by_name[dep.as_str()])
            .find(|dep_index| !processed.contains(dep_index))
            .expect("every unprocessed node has an unprocessed dependency");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    trait Greeter: Service {
        fn greet(&self) -> String;
    }

    struct EngGreeter;

    impl Greeter for EngGreeter {
        fn greet(&self) -> String {
            "hello".into()
        }
    }

    #[allow(dead_code)] // only the key's TypeId is exercised, not the methods
    trait Counter: Service {
        fn bump(&self);
    }

    /// Test plugin recording apply/dispose into a shared log; optionally
    /// provides `dyn Greeter`, and can fail to exercise partial unwinding.
    struct TestPlugin {
        name: String,
        deps: Vec<String>,
        fail: bool,
        provide_greeter: bool,
        log: Arc<Mutex<Vec<String>>>,
    }

    impl Plugin for TestPlugin {
        fn name(&self) -> String {
            self.name.clone()
        }

        fn dependencies(&self) -> Vec<String> {
            self.deps.clone()
        }

        fn apply(self, ctx: &mut Ctx) -> Result<Disposer, PluginError> {
            self.log
                .lock()
                .unwrap()
                .push(format!("apply:{}", self.name));
            if self.fail {
                return Err(PluginError::Apply {
                    plugin: self.name,
                    source: "boom".into(),
                });
            }
            // Registrations must survive `apply` returning: the service
            // disposer is merged into the plugin's own disposer, not dropped.
            let mut disposer = Disposer::new();
            if self.provide_greeter {
                disposer = disposer.extend(ctx.provide::<dyn Greeter>(Arc::new(EngGreeter))?);
            }
            let name = self.name.clone();
            let log = Arc::clone(&self.log);
            Ok(disposer.extend(Disposer::from_fn(move || {
                log.lock().unwrap().push(format!("dispose:{name}"));
            })))
        }
    }

    fn plugin(name: &str, deps: &[&str], log: &Arc<Mutex<Vec<String>>>) -> TestPlugin {
        TestPlugin {
            name: name.to_string(),
            deps: deps.iter().map(|d| d.to_string()).collect(),
            fail: false,
            provide_greeter: false,
            log: Arc::clone(log),
        }
    }

    #[test]
    fn register_and_resolve_service_by_trait_type() {
        let ctx = Ctx::new();
        let _keep = ctx
            .provide::<dyn Greeter>(Arc::new(EngGreeter))
            .expect("first registration succeeds");
        assert!(ctx.contains_service::<dyn Greeter>());
        assert!(!ctx.contains_service::<dyn Counter>());

        let svc = ctx
            .resolve::<dyn Greeter>()
            .expect("registered service resolves by trait type");
        assert_eq!(svc.greet(), "hello");

        assert!(
            ctx.resolve::<dyn Counter>().is_none(),
            "an unregistered trait type must not resolve"
        );
    }

    #[test]
    fn duplicate_service_registration_is_rejected() {
        let ctx = Ctx::new();
        let _first = ctx.provide::<dyn Greeter>(Arc::new(EngGreeter)).unwrap();
        let err = ctx
            .provide::<dyn Greeter>(Arc::new(EngGreeter))
            .expect_err("a stable service key may only be provided once");
        assert!(matches!(err, PluginError::DuplicateService { .. }));
        // The original registration survives the rejected duplicate.
        assert!(ctx.resolve::<dyn Greeter>().is_some());
    }

    #[test]
    fn service_disposer_removes_only_its_own_registration() {
        let ctx = Ctx::new();
        let disposer = ctx.provide::<dyn Greeter>(Arc::new(EngGreeter)).unwrap();
        assert!(ctx.resolve::<dyn Greeter>().is_some());
        disposer.dispose();
        assert!(ctx.resolve::<dyn Greeter>().is_none());
    }

    #[test]
    fn disposer_unwinds_in_reverse_order_exactly_once() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let step = |tag: &'static str, log: &Arc<Mutex<Vec<String>>>| {
            let log = Arc::clone(log);
            Disposer::from_fn(move || log.lock().unwrap().push(format!("dispose:{tag}")))
        };
        let disposer = step("a", &log)
            .extend(step("b", &log))
            .extend(step("c", &log));
        disposer.dispose();
        assert_eq!(
            *log.lock().unwrap(),
            vec![
                "dispose:c".to_string(),
                "dispose:b".to_string(),
                "dispose:a".to_string()
            ],
            "teardown must unwind in reverse insertion order"
        );

        assert!(Disposer::new().is_empty());
        assert!(!Disposer::from_fn(|| ()).is_empty());
    }

    #[test]
    fn disposer_runs_each_callback_exactly_once_on_drop() {
        let counts = Arc::new(Mutex::new([0u32; 3]));
        let mut disposer = Disposer::new();
        for index in 0..3 {
            let counts = Arc::clone(&counts);
            disposer = disposer.extend(Disposer::from_fn(move || {
                counts.lock().unwrap()[index] += 1;
            }));
        }
        drop(disposer);
        assert_eq!(*counts.lock().unwrap(), [1, 1, 1]);
    }

    #[test]
    fn activation_is_topological_and_deterministic() {
        let log = Arc::new(Mutex::new(Vec::new()));
        for _round in 0..2 {
            let mut set = PluginSet::new();
            set.register(plugin("p0", &["p2"], &log));
            set.register(plugin("p1", &[], &log));
            set.register(plugin("p2", &[], &log));

            let mut ctx = Ctx::new();
            let disposer = set.activate(&mut ctx).expect("activation succeeds");
            // Ready set is {p1, p2}; declaration-index tiebreak picks p1
            // first, then p2 unblocks p0 — not declaration order.
            assert_eq!(
                *log.lock().unwrap(),
                [
                    "apply:p1".to_string(),
                    "apply:p2".to_string(),
                    "apply:p0".to_string()
                ],
                "round {_round}: order must be topological and stable"
            );
            log.lock().unwrap().clear();

            disposer.dispose();
            // Disposal unwinds in reverse activation order.
            assert_eq!(
                *log.lock().unwrap(),
                [
                    "dispose:p0".to_string(),
                    "dispose:p2".to_string(),
                    "dispose:p1".to_string()
                ],
                "round {_round}: disposal must unwind in reverse activation order"
            );
            log.lock().unwrap().clear();
        }
    }

    #[test]
    fn dependency_cycle_is_rejected_with_named_error() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut set = PluginSet::new();
        set.register(plugin("a", &["b"], &log));
        set.register(plugin("b", &["c"], &log));
        set.register(plugin("c", &["a"], &log));
        // d hangs off the cycle without being part of it.
        set.register(plugin("d", &["a"], &log));

        let mut ctx = Ctx::new();
        let err = set.activate(&mut ctx).expect_err("cycle must be rejected");
        match err {
            PluginError::DependencyCycle { cycle } => {
                let joined = cycle.join(",");
                for member in ["a", "b", "c"] {
                    assert!(
                        joined.contains(member),
                        "cycle must name {member}: {cycle:?}"
                    );
                }
                assert!(
                    !joined.contains('d'),
                    "cycle must not list non-member d: {cycle:?}"
                );
            }
            other => panic!("expected DependencyCycle, got: {other}"),
        }
        // Nothing was activated, so nothing was disposed either.
        assert!(log.lock().unwrap().is_empty());
    }

    #[test]
    fn unknown_dependency_is_rejected_with_named_error() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut set = PluginSet::new();
        set.register(plugin("a", &["ghost"], &log));
        let mut ctx = Ctx::new();
        let err = set
            .activate(&mut ctx)
            .expect_err("unknown dependency must be rejected");
        match err {
            PluginError::UnknownDependency { plugin, dependency } => {
                assert_eq!(plugin, "a");
                assert_eq!(dependency, "ghost");
            }
            other => panic!("expected UnknownDependency, got: {other}"),
        }
    }

    #[test]
    fn duplicate_plugin_name_is_rejected() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut set = PluginSet::new();
        set.register(plugin("twin", &[], &log));
        set.register(plugin("twin", &[], &log));
        let mut ctx = Ctx::new();
        assert!(matches!(
            set.activate(&mut ctx),
            Err(PluginError::DuplicatePlugin { .. })
        ));
    }

    #[test]
    fn partial_activation_failure_unwinds_in_reverse_exactly_once() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut set = PluginSet::new();
        set.register(plugin("p1", &[], &log));
        set.register(plugin("p2", &["p1"], &log));
        let mut failing = plugin("pfail", &["p2"], &log);
        failing.fail = true;
        set.register(failing);

        let mut ctx = Ctx::new();
        let err = set
            .activate(&mut ctx)
            .expect_err("failing plugin aborts activation");
        match err {
            PluginError::Apply { plugin, .. } => assert_eq!(plugin, "pfail"),
            other => panic!("expected Apply, got: {other}"),
        }
        assert_eq!(
            *log.lock().unwrap(),
            [
                "apply:p1".to_string(),
                "apply:p2".to_string(),
                "apply:pfail".to_string(),
                "dispose:p2".to_string(),
                "dispose:p1".to_string(),
            ],
            "activated plugins must unwind in reverse order, exactly once"
        );
    }

    #[test]
    fn kian_plugins_macro_registers_and_activates() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut greeter_provider = plugin("gp", &[], &log);
        greeter_provider.provide_greeter = true;
        let consumer = plugin("consumer", &["gp"], &log);

        let set = crate::kian_plugins![greeter_provider, consumer];
        assert_eq!(set.names(), vec!["gp".to_string(), "consumer".to_string()]);

        let mut ctx = Ctx::new();
        let disposer = set.activate(&mut ctx).expect("activation succeeds");
        assert_eq!(
            *log.lock().unwrap(),
            ["apply:gp".to_string(), "apply:consumer".to_string()]
        );
        assert!(ctx.resolve::<dyn Greeter>().is_some());

        disposer.dispose();
        assert!(
            ctx.resolve::<dyn Greeter>().is_none(),
            "service disposers wired into the plugin disposer must unwind services"
        );
        assert_eq!(
            *log.lock().unwrap(),
            [
                "apply:gp".to_string(),
                "apply:consumer".to_string(),
                "dispose:consumer".to_string(),
                "dispose:gp".to_string(),
            ]
        );
    }
}
