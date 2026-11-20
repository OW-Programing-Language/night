# OW-native annotated services and controllers (PR #27)

Night now supports opt-in OW `@Service` singletons, `@Controller` route
owners, and **typed `@Inject` field injection**, powered by the OW language
runtime in [ow-lang PR #63](https://github.com/OW-Programing-Language/ow-lang/pull/63).

This is **not Java/Spring reflection**. No Java bean scanner, Spring Boot JARs,
annotation processors or Java classpath dependencies are required to author
services in OW.

## Example

```ow
@Service
class GreetingService:
  fn message() -> string:
    return: "Hello from Night and OW"

@Controller
class GreetingController:
  @Inject
  field private greeting GreetingService

  fn message() -> string:
    return: this.greeting.message()

  fn mount() -> int:
    route "GET /api/greeting":
      set handler: component_get("GreetingController")
      respond: {message: handler.message()}
    return: 1
```

The main composition root **still imports** both OW class declarations:

```ow
import "../../src/NightApp.ow"
import "GreetingService.ow"
import "GreetingController.ow"

main:
  set night: new NightApp()
  call: assert(night.boot() == 1)
  call: assert(night.mount_controllers() == 1)
  call: night.mount_system()
  serve: 8080
```

For the complete example see [examples/controllers](../examples/controllers/main.ow).
Run `python3 tests/controller_http_smoke.py` after compiling the pinned OW runtime.
CI checks the class annotations, typed field injection, singleton identity,
controller mount idempotence and live HTTP route.

## Contract and security boundaries

- `@Service` and `@Controller` apply to OW classes, and typed `@Inject`
  fields refer to **concrete OW annotated classes**. Unknown/unannotated
  dependencies and cycles fail before user `main:` executes.
- Singleton lifetime is the current OW interpreter. Constructors are
  zero-argument; injected fields are populated **after** construction and
  must not be read in `new()`.
- Only **already imported** classes are discovered. This feature does not
  import source files, instantiate OW native functors, or automatically
  register `NIGHT_COMPONENT` modules. `night.discover_native()` remains the
  API for the existing typed module contracts.
- Controller methods aren't converted to HTTP routes automatically:
  each `@Controller` still exposes a public `mount()` method, using OW's
  standard `route` statements. `mount_controllers()` calls those methods
  **once and only after `night.boot()` succeeds**.
- Route handlers don't capture `this` or local lexical variables in the
  current OW HTTP runtime. They can call `component_get("ControllerName")`
  to retrieve the injected singleton.
- Night's security adapter still controls authorization and request
  interception. Annotation scanning does not make an endpoint public or
  disable the existing fail-closed middleware. Handle controller startup
  failures as deployment failures: current OW HTTP routes are not
  transactional and cannot be unregistered by `night.stop()`.
- No auto-generated request binding, parameter decorators, proxy AOP,
  interface binding, per-request component scope, constructor injection, or
  LLVM lowering is implemented in this PR. These are separate future
  parity milestones.
