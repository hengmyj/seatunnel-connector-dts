# Apache SeaTunnel Feature Issue Draft (local only)

> **Status:** Local draft for preview. Do **not** create the GitHub Issue until the author says OK.
>
> **Target repo:** https://github.com/apache/seatunnel
>
> **Template source:** `.github/ISSUE_TEMPLATE/feature-request.yml` (title hint: `[Feature][Module Name] Feature title`; body fields below).
>
> **Suggested title:**
>
> ```text
> [Feature][Connector-V2][Dts] Add Aliyun DTS Subscribe Source connector
> ```
>
> **How to create later (when approved):**
>
> ```bash
> gh issue create -R apache/seatunnel \
>   --title "[Feature][Connector-V2][Dts] Add Aliyun DTS Subscribe Source connector" \
>   --body-file docs/github-issue-feature-dts.md
> ```
>
> Before running `gh issue create`, remove this header / the Chinese appendix, and keep only the English Issue body (from `### Search before asking` through Code of Conduct).

---

### Search before asking

- [x] I had searched in the [feature](https://github.com/apache/seatunnel/issues?q=is%3Aissue+label%3A%22Feature%22) and found no similar feature requirement.

### Description

Please add a **Connector-V2 Source** for **Alibaba Cloud DTS (Data Transmission Service) Subscribe**, so SeaTunnel can consume DTS change-tracking / subscribe channels natively.

#### Motivation

In many enterprise environments:

- Downstream jobs should **not** connect directly to production databases.
- Replay / backfill / multi-consumer workloads must **not** put query or CDC pressure back onto production.
- **DTS Subscribe** is a practical isolation layer: relatively low cost, supports **replay** and **multiple consumer groups**, while keeping production protected.

SeaTunnel already has rich CDC / Kafka / JDBC connectors, but (as of this writing) there is **no first-party Aliyun DTS Subscribe Source** under Connector-V2. Teams today either:

- build ad-hoc bridges (extra process + Socket / Kafka-shaped wrappers), or
- try to treat DTS as a plain Kafka source (auth / Avro / subscribe semantics often do not map cleanly).

A native `Dts` Source would make DTS → SeaTunnel → Sink a first-class, single-process pipeline.

#### Proposed scope (MVP / Source-only)

Initial contribution focus (honest scope; not oversold):

| Area | Plan |
|------|------|
| Connector type | **Source only** (no Sink in the first PR) |
| Module | e.g. `seatunnel-connectors-v2/connector-dts` (name open for discussion) |
| Plugin id | e.g. `Dts` / `seatunnel.source.Dts = connector-dts` (open for discussion) |
| SDK | **Open-source** Maven dependency `com.aliyun.dts:dts-new-subscribe-sdk` ([Maven Central](https://central.sonatype.com/artifact/com.aliyun.dts/dts-new-subscribe-sdk), **Apache-2.0**). Latest observed release at draft time: **2.1.6**. Upstream SDK: [aliyun/aliyun-dts-subscribe-sdk-java](https://github.com/aliyun/aliyun-dts-subscribe-sdk-java) |
| Closed-source jars | **Will not** be committed to the Apache SeaTunnel repository. No proprietary `systemPath` jar in the mainline PR. |
| Output | Envelope `SeaTunnelRow` fields such as `_database`, `_table`, `_op`, `_ts`, `_offset`, `_columns_json` (column name → value JSON from after-image) |
| Filtering | Optional `table-list` whitelist (`db.table`); non-matching DML can be skipped while still advancing the subscribe checkpoint |
| Checkpoint | Configurable start checkpoint (timestamp or `offset@timestamp`), optional force-seek; DTS SDK local store + SeaTunnel job checkpoint coexistence documented |
| Ops knobs | Bounded queue / poll batch sizes; optional dry-run / skip-columns-json **only for benchmarking** (not production defaults) |

Out of scope for the first PR (can be follow-ups):

- Jdbc Sink `schema_save_mode` auto-create / auto-alter from the envelope (needs Transform or a richer CDC-compatible format such as `debezium_json`)
- DDL application / schema evolution into target tables
- Sink connector for DTS
- Claiming exactly-once end-to-end without a clear design review

#### Prior art / reference implementation

A standalone, validated reference implementation already exists outside the Apache tree:

- Repository: https://github.com/hengmyj/seatunnel-connector-dts
- Suggested review branch: `chore/add-chinese-comments`
- It has been exercised against real DTS Subscribe channels (Source → Console / downstream sinks), including table whitelist, checkpoint seek/reset, and throughput experiments.

The Apache PR would be a cleaned-up upstream contribution: **module layout aligned with SeaTunnel conventions**, dependency on the **public Maven SDK only**, docs + examples, and community-agreed naming.

#### Performance note (reference only)

Informal local / staging measurements (not a formal benchmark report; environment and sink matter a lot):

- On roughly **4-core** class machines, **`dry-run=true`** (SDK consume + `commit`, skip convert / enqueue / `_columns_json`) can reach on the order of **100k+ rps** (example observation ~110k+ peak).
- That number is an **upper-bound reference for the lightweight SDK path**, **not** a claim that full `_columns_json` + real Jdbc sink already sustains 100k+ in production.
- Meaningful production throughput needs adequate heap (e.g. multi-GB), sensible `table-list` / subscription scope, no competing consumer on the same `sid`, and a sink that can keep up.

#### Questions for the community

Before / during the PR, feedback would be appreciated on:

1. **Naming:** plugin id `Dts` vs `AliyunDts` / `DtsSubscribe`?
2. **Module path:** `connector-dts` vs `connector-aliyun-dts`?
3. **SDK version:** pin a specific `dts-new-subscribe-sdk` version (e.g. 2.1.6) vs a small range; any dependency conflict concerns (Avro / Kafka clients / Fastjson already used by the SDK)?
4. **Output contract:** keep the envelope + `_columns_json` MVP, or prefer aligning early with an existing CDC JSON / Canal / Debezium-shaped row for easier Jdbc `schema_save_mode` later?

### Usage Scenario

Typical pipelines:

1. **Production isolation:** MySQL/PostgreSQL (or other DTS-supported sources) → **Aliyun DTS Subscribe** → SeaTunnel `Dts` Source → warehouse / ODS / message bus Sink, without opening production DB ports to the integration cluster.
2. **Replay / catch-up:** reset subscribe checkpoint within DTS retention and re-consume for backfill or rebuilding a derived table, using a dedicated consumer group / `sid` so production traffic is undisturbed.
3. **Multi-consumer fan-out:** different SeaTunnel jobs (or environments) use separate DTS consumer credentials / groups for analytics, search indexing, and audit pipelines from the same subscribe channel family.
4. **Narrow sync:** `table-list` whitelist so only a few business tables are converted to rows; other events advance the checkpoint cheaply.

Example (illustrative HOCON; final option names subject to review):

```hocon
env {
  job.mode = "STREAMING"
  parallelism = 1
}

source {
  Dts {
    broker-url = "dts-xxx.aliyuncs.com:18001"
    topic = "your_topic"
    sid = "your_sid"
    user = "your_user"
    password = "your_password"
    checkpoint = "0"
    table-list = ["mydb.orders", "mydb.users"]
  }
}

sink {
  Console {}
}
```

### Related issues

- Searched existing Feature issues for Aliyun DTS / DTS Subscribe Source; did not find a dedicated Connector-V2 request covering this.
- Related ecosystem (not duplicates): Kafka Source, various CDC sources, and user-side bridges — none replace a native DTS Subscribe Source.
- Reference implementation (external): https://github.com/hengmyj/seatunnel-connector-dts (branch `chore/add-chinese-comments`)

### Are you willing to submit a PR?

- [x] Yes I am willing to submit a PR!

(Code will be prepared / cleaned up for Apache contribution after community feedback on naming, module path, and SDK version. Happy to iterate on design in this issue first.)

### Code of Conduct

- [x] I agree to follow this project's [Code of Conduct](https://www.apache.org/foundation/policies/conduct)

---

## 中文摘要（仅本地预览，勿贴进上游 Issue 正文；或按官方提示可附在英文后）

**建议标题：** `[Feature][Connector-V2][Dts] Add Aliyun DTS Subscribe Source connector`

**要点：**

- 企业场景：生产不宜直连；回放不能压生产；DTS 订阅低成本隔离，支持回放 / 多消费组。
- 官方目前没有 Aliyun DTS Subscribe Source。
- 独立仓已有实现验证：https://github.com/hengmyj/seatunnel-connector-dts （分支 `chore/add-chinese-comments`）。
- **明确计划用开源** `com.aliyun.dts:dts-new-subscribe-sdk`（Maven Central，Apache-2.0，草稿时最新约 2.1.6），**不会**把闭源 jar 推进 Apache 主仓。
- MVP：Source、envelope（`_database` / `_table` / `_op` / `_columns_json` 等）、`table-list`、checkpoint；Jdbc `schema_save_mode` / DDL 自动变更 **未做**，可后续。
- 性能：约 4C、`dry-run` 量级 **10W+** 仅作参考，需标注前提（轻路径，非全量 JSON + Jdbc 结论）。
- 愿意后续提交 PR；并询问社区对命名、模块路径、SDK 版本的意见。

**本轮不要**执行 `gh issue create`；用户说「可以了」后再提。
