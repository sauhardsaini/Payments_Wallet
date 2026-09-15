# PayFlow Microservices Project Proposal

Implementation plan for two backend engineers  
Version 1.0 | 16 September 2026

## 1 Project objective

Build a simulated wallet payment platform that both engineers can implement, test, operate and explain in backend interviews. A customer pays a merchant from a funded demo wallet. The system must preserve money and recover correctly when requests repeat, services restart, responses disappear or messages arrive more than once.

The first release contains three independently deployable services: Payment, Financial Core and Notification. Financial Core contains wallet and ledger modules in one service so that a final transfer and its accounting record commit together. Payment owns the workflow across services. Kafka distributes committed outcomes to Notification.

This proposal defines the scope, financial rules, service contracts, ownership, backlog, tests and release evidence. It is a plan for implementation; the acceptance criteria are targets to demonstrate, not claims about an existing system.

Engineer A means you. Engineer B means your friend. The proposed ownership assumes both can develop Java and Spring Boot services. Review the allocation after the first working slice using actual effort.

### Definition of project success

A customer submits an INR 2,500 payment. Financial Core commits the transfer, but its response is lost. Payment restarts and the customer retries. The system returns the original payment, resolves it to success, records exactly one financial transfer and produces one simulated notification.

Both engineers must be able to reproduce this scenario, explain the database and service boundaries, and show the evidence. The portfolio demonstrates specific engineering decisions and measured behaviour; interview level and compensation remain separate hiring decisions.

## 2 Scope and release boundaries

### Release 1 required scope

| Capability | Required behaviour |
|---|---|
| Demo identities and wallets | Seed a customer and merchant, validate caller identity, read authorized wallet balances |
| Funding | Create balanced demo opening entries through an idempotent seed operation |
| Payment API | Create, retrieve and list payments with validation and concurrent duplicate protection |
| Financial operations | Reserve, capture, release and query by a stable payment reference |
| Ledger | Record every finalized movement as one immutable balanced journal transaction |
| Recovery | Persist work before dispatch, resume after crashes, reconcile uncertain outcomes |
| Messaging | Publish Payment outcomes through a transactional outbox and Kafka |
| Notification | Consume events into durable jobs and deliver to a simulated inbox |
| Operations | Show unresolved payments, old holds, retry work and failed notification jobs |
| Verification | Automated database, concurrency, authorization, messaging and fault tests |
| Demonstration | Postman or equivalent scripts plus a small payment and history page |
| Delivery | Docker Compose, CI checks, migrations, runbooks, architecture decisions and measured local load results |

Release 1 uses INR and simulated funds. It excludes real banking credentials, real customer data, card processing, production KYC, multi-currency conversion, fees, refunds and settlement to external banks. Refunds require a separate reversal operation and are not implemented as reservation release.

### Later releases

| Phase | Deliverable | Entry condition |
|---|---|---|
| Release 2 | External Bank Simulator and a separate bank-funded payment journey | Release 1 correctness and restart tests pass |
| Release 3 | Optional identity service, fraud rules, API gateway and justified Redis use | A concrete feature requires each addition |
| Release 4 | Kubernetes and optional AWS deployment with Terraform | Compose startup and operational recovery are reproducible |

These phases are specified in section 17 so the original blueprint's broader learning areas remain available. They do not block the first release. An independent Ledger service is deferred until the team can justify and implement its consistency contract.

## 3 Architecture and service ownership

### Service boundaries

| Component | Owns | Does not own |
|---|---|---|
| Payment Service | Public payment intent, API idempotency, jobs, attempts, workflow history, outcome outbox | Authoritative balances or journal entries |
| Financial Core Service | Customers' and merchants' wallet accounts, holds, financial operation results, journal, demo funding | Public payment orchestration or notifications |
| Notification Service | Kafka inbox, notification jobs, attempts and simulated deliveries | Payment success or financial posting |
| Kafka | Transport for committed payment outcomes | Financial authority or a global transaction |

Request path: client to Payment over HTTP; Payment worker to Financial Core over authenticated HTTP. Event path: Payment database outbox to Kafka to Notification database and simulated inbox. Authorized wallet reads can go directly to Financial Core in the local demo; mutation endpoints are internal.

Each service owns a database and credentials. One local PostgreSQL instance may host three databases. Application roles have no cross-service table access, joins or foreign keys. Financial Core's wallet and ledger modules may share a transaction because they belong to the same service. Keep shared libraries limited to technical helpers and schemas; do not share JPA entities or repositories.

The financial outcome can be committed before Payment learns it. Payment's status is a recoverable workflow view of Financial Core's authoritative result. Notification delays do not change a completed transfer.

### Principal design decisions

1. Keep wallet balances and journal posting in the same transaction boundary.
2. Save payment intent and a processing job before sending a financial command.
3. Use durable idempotency at the public API and every financial operation.
4. Recover uncertain results by stable reference; never interpret a timeout as a definitive decline.
5. Use Kafka for downstream delivery after financial completion.
6. Start with database row locking and a persisted worker. Add more infrastructure only after measuring a need.

## 4 Financial model and invariants

### Money representation

Use integer paise: Java `long`, PostgreSQL `BIGINT`, currency `INR`. INR 2,500 is `250000`. Validate positive amounts, a configurable demo transaction maximum and arithmetic overflow. Set an initial demo maximum of INR 100,000 per payment; this is a project setting, not a banking rule. Use checked arithmetic for derived totals and reject same-wallet transfers in Release 1.

For a customer wallet, `available = posted - held` and `posted >= held >= 0`. Merchant wallets start with zero balance and receive captured payments. A reservation increases held funds without changing posted balances. Capture consumes the hold and transfers posted value. Release removes the hold without moving posted value.

| Stage | Customer posted | Customer held | Customer available | Merchant posted |
|---|---:|---:|---:|---:|
| Demo opening funds | INR 10,000 | INR 0 | INR 10,000 | INR 0 |
| Reserve INR 2,500 | INR 10,000 | INR 2,500 | INR 7,500 | INR 0 |
| Capture that reservation | INR 7,500 | INR 0 | INR 7,500 | INR 2,500 |
| Release instead of capture | INR 10,000 | INR 0 | INR 10,000 | INR 0 |

The last two rows are alternative outcomes of the same reservation.

### Journal authority and demo funding

Represent customer and merchant wallet balances as liability accounts in the simulation. Demo funding debits a demo clearing asset account and credits the customer wallet liability. A customer-to-merchant capture debits the customer wallet liability and credits the merchant wallet liability. The clearing account is a simulation counter-account, not evidence of real cash backing.

For Release 1, use a two-leg `journal_transfers` table: one debit account, one credit account, one positive amount and one currency per immutable row. Expose the two accounting entries through a read-only `ledger_entries` view. This makes each stored journal transaction balanced by construction. Enforce account currency equality and permitted account types in the posting transaction; foreign keys alone do not enforce those rules.

Give each capture a unique `(payment_id, posting_type)` reference and each demo funding operation its own unique reference. Rerunning setup must not add money again. Do not initialize a nonzero balance column without its corresponding journal entry.

Cached posted balances are updated in the same Financial Core transaction as journal posting. Reconciliation compares those balances with credit-minus-debit movements for wallet liability accounts, and checks held totals against active reservations. Run all checks against one consistent Core database snapshot, such as a read-only REPEATABLE READ transaction, so concurrent valid captures do not create false discrepancies. Preserve immutable journal history. Corrections use new referenced entries in a future correction feature, not edits to historical amounts.

### Transaction and lock discipline

Create or obtain the operation row using a unique payment reference, then lock that row. Lock all affected wallet rows in stable wallet-ID order. Validate funds, currency and permitted transition; apply all updates; commit. Never call another service while holding these database locks.

Start with PostgreSQL `READ COMMITTED` and explicit row locks. Lock ordering reduces deadlock risk; still handle a deadlock or transaction retry by rerunning the complete local transaction with a bounded policy. PostgreSQL documents row-lock behaviour and deadlock considerations in its [locking guide](https://www.postgresql.org/docs/current/explicit-locking.html).

## 5 Financial operation contract

Every operation carries `paymentId`, payer wallet, merchant wallet, amount and currency. These immutable fields form a canonical fingerprint. Financial Core rejects a changed fingerprint for an existing payment reference.

| Operation | First valid effect | Repeat or conflicting request |
|---|---|---|
| Reserve | Lock records, check available funds, increase held, store `RESERVED` | Same details return current outcome; insufficient funds persist `REJECTED` |
| Capture | From `RESERVED`, atomically debit payer, credit merchant, consume hold, insert journal and store `CAPTURED` | Duplicate returns original journal reference; released or rejected operations cannot capture |
| Release | From `RESERVED`, reduce held and store `RELEASED` | Duplicate has no effect; after capture, return the captured outcome without reversing it |
| Query | Read authoritative operation and journal reference | Unknown reference returns 404, which is not proof that an in-flight request cannot still commit |

Terminal Core states are `CAPTURED`, `RELEASED` and `REJECTED`. They cannot be reopened. A later attempt to pay after insufficient funds uses a new public payment intent and key.

Release arriving before reserve creates a `RELEASED` tombstone with the immutable fingerprint. A delayed reserve then observes it and cannot create a new hold. Capture arriving before reserve has no financial effect and returns a state conflict. Concurrent capture and release serialize on the same operation row; exactly one terminal result wins. If capture wins, Payment must report success.

Return current authoritative state for a matching operation; use `409` with state details for an incompatible action or changed fingerprint. Repeat requests against already achieved outcomes return success without repeating the mutation. Record business rejection results durably so that retries do not silently turn a previously rejected intent into a payment.

No automatic hold expiry runs independently in Release 1. Old holds appear in an operational queue. Any future expiry must invoke the same serialized release transition so it cannot race unsafely with capture.

## 6 Payment lifecycle and durable execution

### Public state contract

| State | Meaning | Permitted next outcome |
|---|---|---|
| `PENDING` | Intent and processing job are committed | Processing, or a persisted pre-financial decline |
| `PROCESSING` | Worker is executing reserve and capture | Success, definitive rejection or reconciliation |
| `RECONCILING` | A dispatched action has an uncertain result | Resume processing or adopt a confirmed terminal result |
| `SUCCEEDED` | Financial Core confirms `CAPTURED` | Terminal |
| `DECLINED` | Definitive business rejection, with no captured funds | Terminal |
| `CANCELLED` | Financial Core confirms `RELEASED` | Terminal |

Store a separate `workflow_step` such as `RESERVE`, `CAPTURE`, `QUERY` or `RELEASE`, plus a persisted `abort_requested` decision. `review_required` is an operational flag, not a fabricated financial outcome. Do not use a generic terminal `FAILED` state for timeouts, Kafka outages or exhausted retries.

### Normal processing

1. Validate identity and request; atomically create idempotency record, payment intent and job. Return the stable payment reference after commit.
2. Claim due work in a short database transaction with an expiring lease and a new claim token.
3. Persist the next financial action before dispatch. Call Financial Core outside the database transaction, reusing the same payment reference.
4. Reserve funds. Persist the observed result and next action. A confirmed rejection becomes `DECLINED`.
5. Capture the reservation. A confirmed `CAPTURED` result supplies the immutable journal reference.
6. Commit `SUCCEEDED`, workflow history and its completion outbox event together in Payment's database.
7. Relay the event to Kafka. Notification creates durable delivery work independently.

### Restarts and uncertain results

Persist attempt count, next retry time, last error category, lease owner/expiry and claim token. Use database time for leases. A worker may claim eligible rows using `FOR UPDATE SKIP LOCKED`; a conditional update using the claim token fences stale local state writes.

After a timeout or crash, query Core by payment ID. If it is captured, finalize success. If reserved, resume the persisted capture or abort decision. If released or rejected, adopt that final result. If absent, safely repeat the original command with the same reference, or issue the persisted release decision that creates a tombstone. A query returning 404 must never trigger an unqualified declaration of failure.

A lease is not remote cancellation: a paused stale worker can still send an HTTP request after losing its lease. Core's idempotency, lock discipline and terminal states provide the final protection against duplicate or contradictory mutations.

Use configurable demo defaults: 2-second HTTP response timeout; retries after approximately 2, 4, 8, 16 and 30 seconds, with jitter; then mark `review_required` and stop scheduled retries. These are starting settings, not production SLOs. A bounded operator action can schedule another query attempt after the dependency recovers. Persist every manual resume and abort with actor, reason and time.

An abort persists the decision before dispatching release and remains unresolved until Core confirms release. Capture may win a race with abort; in that case the correct result is `SUCCEEDED`. A refund would be a separate future business operation.

## 7 API and event contracts

Agree and version OpenAPI and event schemas before either engineer codes cross-service calls. Store examples for success, duplicate request, conflicting payload, decline and uncertain outcome. Identity must come from a validated token; do not trust a customer ID supplied in the request body. For each new intent, resolve the merchant to a wallet through a Core reference read and persist that resolved wallet ID before financial dispatch. Existing-key retries use the stored mapping and compare the original request fields, rather than re-resolving a potentially changed merchant mapping.

### External API

| Endpoint | Contract |
|---|---|
| `POST /v1/payments` | Require `Idempotency-Key`; accept payer wallet, merchant, integer amount and INR; return 202 after durable acceptance |
| `GET /v1/payments/{id}` | Return authorized current state, timestamps, review flag and journal reference when confirmed |
| `GET /v1/payments?cursor=...` | Return caller-visible history using stable cursor pagination |
| `GET /v1/wallets/{id}` | Return authorized posted, held and available balances from Core |
| `GET /v1/notifications` | Return the caller's simulated inbox |
| `POST /internal/payments/{id}/resume` | Operator-only audited scheduling of recovery |
| `POST /internal/payments/{id}/abort` | Operator-only audited abort request; may resolve to success if capture already won |

Example creation body:

```json
{
  "payerWalletId": "demo-customer-wallet",
  "merchantId": "demo-merchant",
  "amountMinor": 250000,
  "currency": "INR"
}
```

The authenticated principal and endpoint scope plus idempotency key identify an intent. A new key creates one payment. Reusing the same key and normalized details returns the original 202 acknowledgement and payment ID; use GET for live state. Changed details return 409. Validate basic input before creating an intent, and leave no payment/job for rejected malformed input.

Enforce uniqueness at the database boundary and create intent plus job atomically. Use a conflict-safe insert and explicit transaction handling rather than catching a uniqueness error and continuing inside a failed transaction. Canonical fingerprints include the validated owner, payer wallet, merchant, amount and currency. Retain idempotency records for the life of the Release 1 demo database. Expiry policy is later work.

Errors use a consistent envelope with `code`, `message`, `paymentId` when applicable and `traceId`. Distinguish 400 validation, 401 authentication, 403/404 authorization policy, 409 conflict, 429 rate limit if enabled, and 503 unavailable before acceptance. A client that loses the acceptance response retries with the same key.

### Internal financial API

Use `POST /internal/transfers/{paymentId}/reserve`, `/capture` and `/release`, and `GET /internal/transfers/{paymentId}`. Carry the immutable fingerprint fields on mutations; return `paymentId`, `state`, reason code and `journalId` when captured. Only the trusted Payment caller can mutate; Core validates the delegated owner and wallet relationship as well as the service credential.

### Event envelope

Use `payment.outcomes.v1` with `paymentId` as partition key. Release 1 publishes only terminal outcomes such as `PaymentSucceeded`, `PaymentDeclined` and `PaymentCancelled`. Include stable `eventId`, `eventType`, `schemaVersion`, `paymentId`, `aggregateVersion`, `occurredAt`, recipient reference and trace context. A success includes amount, currency and journal reference; avoid embedding credentials or full personal data.

Consumers ignore duplicate event IDs and do not regress an already applied aggregate version. Preserve outbox order per payment when publishing. Avoid claiming global ordering across partitions. Prefer additive compatible schema changes and keep contract fixtures in the same PR as the producer change.

## 8 Persistence design

Use Flyway migrations for every database change. Disable automatic production-style schema mutation by Hibernate; validate schema at startup. Entity names below are the implementation contract to refine into migrations, not complete DDL.

| Database | Tables or views | Essential constraints |
|---|---|---|
| Payment | `payments`, `idempotency_keys`, `payment_jobs` | Unique owner/scope/key; unique job per payment; immutable intent fields |
| Payment | `payment_attempts`, `payment_events`, `outbox_events` | Unique event ID and outcome version; foreign keys within Payment DB |
| Financial Core | `wallets`, `accounts`, `financial_operations` | Unique payment reference; currency and positive amount validation; posted >= held >= 0 |
| Financial Core | `journal_transfers`, `ledger_entries` view | Unique posting reference; positive amount; different debit/credit accounts; append-only journal |
| Financial Core | `funding_operations`, `reconciliation_runs` | Unique funding reference; recorded discrepancy evidence |
| Notification | `consumer_inbox`, `notification_jobs` | Unique consumer/event ID; unique outcome/channel/recipient delivery intent |
| Notification | `notification_attempts`, `mock_deliveries`, `quarantined_events` | Unique delivery reference; durable replay and failure metadata |

Index Payment history by owner, creation time and ID; due jobs by status and next attempt time; unresolved payments by state and age; unpublished outbox by publication status and sequence; active holds by wallet/state; journal references by payment and account history. Use stable `(created_at, id)` cursors and check query plans against seeded data.

Capture and funding atomically commit their financial result, balance caches and journal. Reserve and release atomically commit operation state and held-balance changes without posting money. The two-leg journal model avoids pretending that a normal row CHECK can enforce a sum across multiple ledger rows. If fees or multi-leg accounting are added later, replace it deliberately with a balanced posting model and appropriate transaction-level enforcement.

Keep runtime users from updating or deleting journal history through normal APIs. Use separate migration credentials. Cross-service consistency checks use service APIs or purpose-built exports; do not give Payment direct access to Core tables.

## 9 Kafka outbox and notification reliability

Payment commits its terminal status and outbox row together. A relay publishes committed rows and marks them published only after broker acknowledgement. A crash after publish but before marking can cause redelivery; the event ID remains unchanged. Monitor oldest unpublished age and relay errors. PostgreSQL-to-Kafka outbox semantics and duplicate-consumer requirements are described in [AWS transactional outbox guidance](https://docs.aws.amazon.com/prescriptive-guidance/latest/cloud-design-patterns/transactional-outbox.html).

Notification consumes an event, inserts its inbox record and creates the delivery job in one local transaction, then commits the Kafka offset. If it crashes after the database commit but before the offset commit, redelivery finds the inbox record and creates no second job. Disable offset behaviour that can acknowledge a message before this durable step.

For Release 1, delivery means inserting a uniquely keyed record in `mock_deliveries` and marking the job delivered in the same database transaction. This allows a demonstrable one-delivery outcome. A future email/SMS adapter needs provider idempotency or a documented possibility of duplicate external delivery after response loss.

Store transient retries with backoff. Quarantine malformed or permanently invalid messages durably before advancing past them; record source topic, partition, offset, schema version, safe error details and event ID when available. A corrected replay preserves original business identity and records an audit entry. Implement a Kafka DLQ topic only with a durable publication path; writing to a DLQ and committing an offset introduces another failure boundary.

Kafka outages do not undo committed wallet payments. Payment accumulates outbox work and applies a configurable backlog limit before accepting further work if storage is at risk. During recovery, the relay drains the backlog and notifications catch up. No money movement is triggered by Notification or by replaying an outcome event.

## 10 Security and operational behaviour

### Required security controls

Seed demo identities and use a local test issuer or development identity provider. Each exposed service validates token signature, issuer, audience and expiry. Customers can access only their own wallets/payments; merchants can access only their received-payment view. Validate beneficiary/merchant mapping before accepting financial instructions.

Use service authentication for internal Core commands and propagate verifiable caller context. A spoofable identity header alone is insufficient. Keep mutation endpoints off the public network. Apply request validation, size limits and bounded pagination. Store credentials in local environment files excluded from Git and provide sanitized example configuration. Enable TLS for any deployment beyond the isolated local environment.

Fault controls, demo funding and database reset are development-only features. Do not expose them in a deployed public profile. Log IDs, state and error categories without tokens, passwords or full request payloads. A future registration service must add password hashing, account lifecycle and token issuance; Release 1 does not require building an identity provider.

### Signals and runbooks

| Signal | Operational action |
|---|---|
| Age of unresolved payments or old holds | Inspect Core by stable reference, then schedule audited resume or abort |
| Oldest unpublished outbox row | Restore broker/relay, verify acknowledgement and drain without regenerating IDs |
| Kafka lag or notification retries | Inspect consumer health, database and quarantined payload metadata |
| Balance versus journal discrepancy | Record evidence, disable affected wallet mutations, investigate; do not silently patch balances |
| Repeated lease expiry or stale updates | Inspect slow dependencies and worker health; verify claim fencing |

Expose liveness for the process and readiness for the dependencies needed to serve its role. Kafka failure alone need not make Payment unready while durable acceptance remains safe; database failure does. Bound this with the outbox capacity policy. Avoid restarting a healthy application repeatedly because an external dependency is down.

Propagate trace context through HTTP and events. Log `paymentId`, operation state, attempt and trace ID. Use low-cardinality metric labels; do not put payment IDs into metric labels. Track API latency, financial completion latency, retry count, unresolved age, lock waits, outbox age, Kafka lag and invariant-check results.

## 11 Technology and repository baseline

| Area | Release 1 choice | Rationale |
|---|---|---|
| Runtime | Java 21 and a compatible maintained Spring Boot release | Shared Java baseline and mature application tooling |
| Build | Maven wrapper and Spring dependency management | Reproducible versions on both machines and CI |
| Persistence | PostgreSQL, Flyway, JPA with explicit SQL where required | Transactions plus visible control over locks and conflict handling |
| Messaging | Kafka and Spring Kafka | Durable downstream event delivery |
| Testing | Framework-managed JUnit, Mockito where useful, Testcontainers | Real PostgreSQL/Kafka integration and deterministic fault cases |
| Execution | Docker Compose; persisted worker jobs | Reproducible local environment and restart recovery |
| Visibility | Structured logs, Actuator/Micrometer and OpenTelemetry | Inspect latency, backlogs and traces across boundaries |
| Client | Postman or scripts, then a small React or plain web client | Demonstrate the complete intent lifecycle |
| CI | GitHub Actions or equivalent repository CI | Build, contracts, tests and reproducible images |

At kickoff, pin exact compatible versions and container images in the repository, including CI. Avoid floating `latest` tags. The [Testcontainers PostgreSQL module](https://java.testcontainers.org/modules/databases/postgres/) provides database integration support. Do not substitute H2 for tests of PostgreSQL locking and constraints.

```text
payflow/
  services/payment-service/
  services/financial-core-service/
  services/notification-service/
  client/
  contracts/openapi/
  contracts/events/
  infrastructure/compose/
  tests/integration/
  tests/failure/
  tests/load/
  docs/architecture/
  docs/adr/
  docs/runbooks/
  .github/workflows/
  compose.yaml
  README.md
```

Use `main` as the demo-ready branch and short-lived `feature/*` branches. Each PR changes one behaviour and includes migrations, contract changes and meaningful tests. Require peer review and CI before merge. Introduce a separate integration branch only if your release workflow actually needs it.

## 12 Balanced ownership

Ownership includes implementation, migrations, tests, API documentation, runbooks and a demo. Every item has one delivery owner and the other engineer as reviewer. Both engineers must understand the full payment flow.

| Area | Primary owner | Reviewer | Evidence the owner should explain |
|---|---|---|---|
| Payment intent and API idempotency | A | B | Concurrent duplicate protection and durable acceptance |
| Payment worker and recovery | A | B | Leases, claim fencing, unknown results and audited resumption |
| Financial Core and accounting | B | A | Reservation, capture, release, funding and balanced posting |
| Financial concurrency | B | A | Lock order, overspend prevention and capture/release races |
| Notification and Payment outbox | A | B | Crash-safe publishing, inbox deduplication and simulated delivery |
| Local runtime and fixtures | B | A | Separate service databases, reproducible migrations and startup |
| CI and application security | A | B | Auth boundaries and automated checks |
| Fault harness and load experiment | B | A | Repeatable response loss, restart and concurrent-spend tests |
| Thin client | A | B | Stable intent key and clear unresolved-status display |
| Observability and documents | A owns workflow views; B owns financial views | Cross-review | Traces, invariant reports and recovery runbooks |

B's financial work carries substantial transaction and concurrency complexity. A owns the smaller Notification service, client and CI/security wiring to balance the workload. Do not allocate work by service count alone. When one engineer waits on an API, use reviewed contract fixtures and spend the available time on tests or another independent item.

Hold one weekly integration and failure-demo session. Exchange a short recorded demo when schedules do not overlap. Cross-review all money-changing code and all retry/abort behaviour. Rebalance at each milestone based on completed tasks and remaining complexity.

## 13 Delivery milestones and effort assumptions

Use 10 to 14 weeks at about 10 to 12 focused hours per person per week as an initial planning range, roughly 200 to 336 combined hours. This includes review, tests and documentation. It is an estimate for Release 1, not a promised delivery date; revisit it after the first two weeks. Learning unfamiliar Spring, Kafka or accounting concepts may extend it. Releases 2 to 4 are additional work.

| Milestone | A workstream | B workstream | Exit evidence |
|---|---|---|---|
| M0 week 1 | Payment contracts and CI outline | Financial rules and local runtime | Reviewed contracts and runnable service shells |
| M1 weeks 2 to 3 | Durable payment create/read and auth boundary | Balanced funding, wallet reads and reservation | Duplicate requests and concurrent holds pass |
| M2 weeks 4 to 5 | Payment-to-Core workflow | Atomic capture and release | One payment matches balances and journal |
| M3 weeks 6 to 8 | Durable recovery, outbox and operator actions | Race hardening and fault harness | Lost response plus restart resolves one transfer |
| M4 weeks 9 to 10 | Notification, client skeleton and workflow telemetry | End-to-end tests and financial telemetry | Event recovery and operational views work |
| M5 weeks 11 to 14 if needed | Client polish, fixes and service documents | Load evidence, release coordination and documents | Fresh-checkout demo and all release gates pass |

Tests and basic logs begin with the first behaviour; the later milestone expands their coverage. CI begins at bootstrap. Kafka-dependent downstream work follows the financial correctness slice. No optional infrastructure task takes priority over a failing financial test.

## 14 Implementation backlog

Each row includes code, migrations where applicable, tests, peer review and documentation. Dependencies are task IDs. A owns A rows and B reviews them; B owns B rows and A reviews them. Joint design tasks still have a named coordinator.

### Foundation and financial slice

| Task and owner | Deliverable | Depends on | Acceptance evidence |
|---|---|---|---|
| PF01 A with B | Scope, state machines, invariants and decisions | None | Both explain completion, timeout and abort rules |
| PF02 A with B | OpenAPI and event contracts; B authors Core contract | PF01 | Fixtures cover duplicates, conflicts, decline and uncertainty |
| PF03 B | Service shells, Compose, separate DB roles, migrations | PF01 | Fresh startup works and cross-service table access fails |
| PF04 A | Build/test CI and sanitized configuration | PF03 | Clean runner passes; deliberate test failure blocks merge |
| PF05 B | Account, journal and funding schema; balance reads | PF02 PF03 | Repeated seeding funds once; journal and balance reconcile |
| PF06 A | Payment create/read/list, idempotency and job transaction | PF02 PF03 | Twenty same-key requests create one payment and job |
| PF07 B | Reserve, release, query and released tombstone | PF05 | Holds cannot overspend; delayed reserve cannot reopen release |
| PF08 B | Atomic capture, merchant credit and journal | PF07 | Duplicate capture posts once; pre-commit crash rolls back all effects |
| PF09 A | Worker-driven reserve/capture and confirmed outcomes | PF06 PF08 | Full wallet payment matches both balances and journal |

### Recovery and release

| Task and owner | Deliverable | Depends on | Acceptance evidence |
|---|---|---|---|
| PF10 A | Leases, claim tokens, backoff, query recovery and audited abort | PF09 | Restart, response loss and stale workers preserve result |
| PF11 B | Capture/release races, invariant checker and audited wallet suspension | PF08 | One race winner; discrepancy detected; suspended wallet rejects new reserve/capture |
| PF12 A | Payment outcome outbox and relay | PF10 | Commit/publish crashes lose no event; IDs survive redelivery |
| PF13 A | Notification inbox, jobs, mock delivery and audited quarantine replay | PF12 | Consumer crashes and duplicates yield one mock delivery |
| PF14 A | Complete customer, merchant, operator and service authorization | PF06 PF09 | Cross-owner access and untrusted mutations are rejected |
| PF15 B | Deterministic fault and concurrency suite | PF10 PF11 PF13 PF14 | All section 15 correctness scenarios run unattended |
| PF16 A with B | Health, tracing, metrics and unresolved/old-hold views | PF10 PF11 PF13 | Trace one payment and demonstrate backlog/invariant signals |
| PF17 A with B | Thin client, rehearsal scripts and local load report | PF14 PF15 PF16 | Same intent retains key; report includes workload and limits |
| PF18 B with A | Release review, runbooks, decisions and contribution notes | PF04 PF15 PF16 PF17 | Both reproduce clean startup and the other's recovery demo |

PF14 completes and tests authorization; the minimum authentication and ownership checks are part of PF05, PF06 and PF09 from the beginning. A client skeleton can start after PF14; PF17 dependencies gate the final integrated client and load evidence. PF15 consolidates fault automation; individual fault tests are part of PF07 through PF13.

### First working week

1. Both agree wallet journey, funding model, ownership and weekly time budget. A records workflow decisions; B records money invariants.
2. A drafts Payment requests and errors. B drafts reserve/capture/release/query. Review references, terminal states and timeout semantics together.
3. B creates local service/runtime shells and DB users. A creates CI and environment examples.
4. A persists intent plus job. B implements idempotent funding and balance reads. Cross-review constraints.
5. Demonstrate clean startup, duplicate request protection and balanced opening funds. Record what remains incomplete and resize the next milestone.

## 15 Verification and release gates

Use unit tests for state transitions, validation and arithmetic. Use Testcontainers for real PostgreSQL and Kafka boundaries. Use HTTP fault controls that deterministically delay a response after commit or fail before processing. Fault controls must be disabled outside test/demo profiles. Avoid relying on arbitrary sleeps to provoke a race; use barriers and explicit synchronization.

| Test | Required assertion |
|---|---|
| Twenty identical concurrent creates | One payment, one job, one operation and one capture journal |
| Same key with changed details | 409 and no second financial effect |
| Different keys compete for funds | Available balance never negative; only affordable holds succeed |
| Crash after acceptance before dispatch | Restart resumes the committed job |
| Reserve commits then response is lost | Recovery observes one hold and does not reserve twice |
| Capture commits then response is lost | One transfer; Payment eventually becomes `SUCCEEDED` |
| Crash before capture commit | No partial debit, credit, hold consumption or journal |
| Release arrives before reserve | Tombstone prevents a later hold |
| Concurrent capture and release | One terminal result, valid balances and matching journal |
| Stale worker resumes after lease loss | No status regression or duplicate mutation |
| Retry budget exhausted | State remains unresolved and review work is visible |
| Payment DB unavailable after Core capture | Later recovery adopts the committed Core outcome |
| Broker unavailable | Financial processing can finish; committed outbox drains after restoration |
| Relay crashes after publish | Redelivery retains event identity and causes one downstream job |
| Consumer crashes around DB commit | No lost job and no duplicate delivery intent |
| Duplicate or stale outcome event | No duplicate delivery or state regression |
| Invalid event followed by valid event | Invalid event is inspectable; valid work continues |
| Repeated demo seed | No duplicate opening funds |
| Unauthorized caller or spoofed identity | No cross-owner reads or financial mutation |
| Reconciliation after every fault run | Zero unexplained posted, held or journal discrepancies |

Assert persisted effects, not just HTTP codes or log messages. Distinguish one capture journal from the separate opening-funding journal. For history and ledger verification, retain stable references to every expected operation.

### Performance experiment

Start with a reproducible experiment such as 1,000 payment intents at concurrency levels 1, 10 and 25. Use enough separately funded customers for a throughput experiment, then a shared-wallet workload specifically for contention. Include repeat keys in a separate workload. Treat these as proposed workloads, not capacity claims.

Report hardware/container resources, versions, data size, workload mix, duration, request acceptance latency, time to financial completion, errors, retry count, lock waits and invariant results. Set performance targets after the initial baseline and repeat under the same conditions. A high request rate with incorrect balances is a failed experiment.

### Release definition of done

Release only when the required automated scenarios pass, all accepted intents are either terminal or explicitly visible for review, no unexplained money discrepancy remains, and both engineers can follow the runbooks from a fresh checkout. Document any remaining operational limitation. Tag the tested revision and preserve the exact demo scripts and load report.

## 16 Operational runbooks and interview evidence

Write short executable runbooks with prerequisites, commands, expected records, recovery steps and verification. Required runbooks cover startup and seed, payment uncertainty, old holds, outbox backlog, notification quarantine/replay, invariant discrepancy and development-only reset.

For uncertain payment recovery, inspect Payment and Core by the same ID, record the authoritative state, schedule a bounded query or persisted abort, then verify final status, journal and balances. Never repair a payment by independently editing state columns in multiple services. For an invariant discrepancy, use an audited operator action to suspend new reserves and captures on the affected wallet, preserve evidence and investigate the transaction boundary. Core checks suspension within the same locked transaction as the proposed mutation. Inspect or release an existing hold only through the controlled recovery procedure; never force capture or change balances manually. Unfreeze only after correction, successful reconciliation and a recorded operator review.

The 10 to 15 minute demo should show a normal payment, the same request repeated, two payments competing for funds, a committed capture with response loss and restart, Kafka recovery, a duplicate outcome event and one trace across the services. Show the invariant report after faults. Kubernetes screenshots are optional later-release evidence.

Each engineer keeps contribution notes for three substantial decisions: problem, implementation, alternative considered, failure test and measured result. A can discuss durable orchestration, API idempotency and messaging. B can discuss financial invariants, locking and atomic journal posting. Each must also demonstrate recovery in the other's service. Resume statements must describe work actually completed and measured.

## 17 Extension plan

### Release 2 External Bank Simulator

This phase implements the original bank-processing project as a separate `BANK_SIMULATED` rail. Release 1 payments use `WALLET`. The rail is immutable and included in the intent fingerprint. A wallet payment must not additionally debit a simulated bank account.

Add a Bank Simulator service with its own database, unique merchant reference, immutable request fingerprint, result lookup and deterministic failure modes. For the first version, it models bank execution and transfer results, without pretending to maintain real bank balances. Keep wallet journal rules limited to the wallet rail; designing external-bank settlement accounting is separate work.

| Task | Owner and reviewer | Acceptance evidence |
|---|---|---|
| EX01 Separate rail and bank contract | A owns, B reviews | Wallet route unchanged; bank route never calls wallet capture |
| EX02 Simulator processing and lookup | B owns, A reviews | Same reference processes once; changed payload conflicts |
| EX03 Bank adapter and recovery | A owns, B reviews | Bank success then timeout resolves through status lookup |
| EX04 Failure and restart suite | B owns, A reviews | Decline, pre-processing failure, delayed success and persistent uncertainty behave correctly |

Use the payment ID as stable bank reference. Simulate success, definitive decline, rejection before processing, success followed by a delayed response, and unresolved status. Repeated safe processing uses the same reference. Exhausted retries remain visible as uncertain. A later settlement feature must state which accounts are debited and credited before adding journal entries.

### Release 3 Identity risk and edge features

| Addition | Primary owner | Required work and completion evidence |
|---|---|---|
| Identity service or integrated identity provider | A, B reviews | Registration/login if needed, password hashing when owned, token validation and authorization regression tests |
| Fraud rules module then optional service | B, A reviews | Versioned decision by payment reference; explicit timeout policy; evaluate before funds dispatch |
| API gateway | A, B reviews | Routing, validated identity propagation, rate limits and direct-service authorization tests |
| Redis | A, B reviews | One measured need such as rate limiting/cache; outage test; PostgreSQL remains financial authority |
| Refunds or limits | B, A reviews | Separate operation references, accounting specification, permissions and reversal/concurrency tests |

For a fraud outage, keep the payment pending review before any money-changing call; define bounded retry. Do not quietly introduce a fail-open policy. Splitting the module into a service requires durable decisions and contract tests. Retain a written reason for each new service. A dedicated Ledger service requires a new authority and reconciliation design before extraction.

### Release 4 Kubernetes and cloud

B owns deployment manifests and infrastructure; A owns deployment CI and operational checks; both review secrets, cost assumptions and recovery. First deploy to a local Kubernetes cluster. Add readiness/liveness probes, resource requests/limits, graceful shutdown, rolling updates and a two-worker recovery test. Demonstrate that pod termination does not lose financial work.

AWS is optional. Before provisioning, compare a small container deployment with EKS and choose against a stated learning objective and budget. Define networking, database backups, service identities, secret storage, Kafka hosting, logs, estimated costs and teardown. Use Terraform, restricted inbound access and an explicit destruction procedure. Do not assume one modest cluster is free or production highly available.

Release evidence includes image/version pins, infrastructure plan, deployment health checks, application recovery under rolling restart, a database backup/restore drill, observed resource use and confirmed cleanup. Include these tasks in a separate estimate after Release 1; do not add them to its original hours silently.

## 18 Architecture decisions and kickoff checklist

Create short ADRs recording context, decision, alternatives, consequences and test evidence. Required decisions are service boundaries; wallet and ledger transaction scope; integer money and two-leg journal; row locking; idempotency/key retention; payment finality and abort races; worker leases; outbox/inbox and ordering; identity/trust boundaries; and optional infrastructure justification.

The implementation defaults are defined in this proposal. At kickoff, both engineers record exact versions, weekly availability, initial backlog estimates, local machine capacity and the demo maximum amount. Choose a test identity issuer and the operator authentication method. Record these routine configuration choices in the repository; they should not postpone building the first slice.

The release artifacts are source code, contracts, migrations, tests, repeatable local setup, runbooks, architecture diagrams, ADRs, demo scripts, a measured load report and individual contribution notes. Markdown is the editable source of this proposal; the Word copy contains the same substantive plan.

## 19 Reference material

- PayFlow Professional Project Blueprint, supplied proposal, especially pages 3 to 10, 14, 19 to 22 and 28 to 30. Used as the starting scope for this revised design.
- [PostgreSQL explicit locking](https://www.postgresql.org/docs/current/explicit-locking.html), for row locks and deadlocks.
- [AWS transactional outbox pattern](https://docs.aws.amazon.com/prescriptive-guidance/latest/cloud-design-patterns/transactional-outbox.html), for durable publication and consumer duplication considerations.
- [AWS saga orchestration pattern](https://docs.aws.amazon.com/prescriptive-guidance/latest/cloud-design-patterns/saga-orchestration.html), for coordinating local transactions and idempotent participants.
- [Stripe idempotent requests](https://docs.stripe.com/api/idempotent_requests), for an example of stable keys and request-parameter comparison. PayFlow's explicit API contract governs its own behaviour.
- [Testcontainers PostgreSQL module](https://java.testcontainers.org/modules/databases/postgres/), for real database integration tests.

