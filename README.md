# RailWatch 铁路新闻证据台

RailWatch 服务铁路运营信息分析员。它保存已登记来源的新闻原文，提取可由原文引句核对的指标主张，并要求审核员以范围版本提交决议。若同一指标存在冲突，当前范围保持未解决且不能发布。发布后的证据快照包含来源链接、引文与稳定指纹，不受后续文章版本影响。

系统使用 SQLite 持久化和 FastAPI 接口。网络采集是可选运行路径；测试与演示使用本地合成数据，不依赖新闻网站在线状态。演示中的 example.org 文字与数值不是新闻报道事实。

## 本地验收

预装 requirements.txt 中锁定的依赖后，在项目根目录执行：

    python -m unittest discover -s tests -v
    python -m compileall -q railwatch
    python -m railwatch.demo

演示打印采集 ID、审核范围版本、快照摘要、数值与溯源条数；业务断言不成立时以非零状态退出。

## 接口与输入

设置 RAILWATCH_DB 为可写的 SQLite 路径，使用 ASGI 服务启动 railwatch.api:app。运行时可在 /docs 查看 OpenAPI。主要接口包括来源策略、文章回放与批量回放、审核队列与候选比较、审核决议、范围、发布快照、当前及历史报表、采集任务和质量诊断。

文章输入包括来源 ID、文章外部 ID、原文 URL、带时区的采集和发布时间、标题、正文、媒体类型和元数据。JSON 正文包含发布文本 text 和带引句的 claims 数组。主张按项目、类型、指标和期间归属，数值类需有受支持的单位；里程碑类需有状态。缺失引句或非法字段会隔离文章，不会进入报告。

## 解析规则版本与离线重处理

发布方把铁路指标从行内 `[RAILCLAIM {...}]` 标记迁移到新版 JSON 字段 `railway_indicators` 后，提取器通过可标识的规则版本区分两套规则：

- 规则 v1（`legacy-inline-markers`）：读取行内标记与旧版 `claims` 数组。
- 规则 v2（`railway-indicators-json`）：JSON 正文优先读取 `railway_indicators`，缺失时回退到 `claims`；行内标记规则两版本保持一致。

每次解析都在 `parse_attempts` 表留下一条追加记录，含规则版本、规则摘要指纹 `rule_digest`、状态（parsed/quarantined）、结果指纹、失败原因与触发方式。同 `(capture, rule_version)` 重复处理幂等：直接回放已有尝试，不重新请求站点、不新增行、不改变状态。是否需要处理只由该版本的尝试记录决定，曾被旧规则解析过的正文不会被误判为“已完成”。

离线重处理只读取数据库中保存的原文，不发起任何网络请求。新规则产出的主张若与当前活跃主张不同，旧主张置 `superseded`、引用它的审核范围回到未解决并自增版本，发布资格被阻止，需重新人工审核；已冻结的发布快照内容不变，仍可按当时结果重现。人工复核历史（reviews）与新旧两版解析尝试始终保留。质量诊断会把由旧规则产出的已通过证据标记为 `rule_changed`。

主要接口：`POST /captures/{id}/reprocess`（指定 `rule_version` 单篇重处理）、`POST /captures/reprocess-pending`（批量补处理缺少该版本尝试的现存原文，清单在批次开始时确定，处理过程中新入库的抓取不会被本轮捞起）、`GET /captures/{id}/parse-attempts`（解析历史与失败原因）、`GET /claims/{id}/reviews`（人工复核历史）。CLI 对应 `reprocess`、`reprocess-pending`、`attempts`、`review-history` 子命令。旧数据库首次打开时自动幂等迁移，历史主张默认归属规则 v1。

审核员提交接受、拒绝或暂缓决议，同时提供审核人、理由和当前范围版本。旧版本决议失败。发布要求所有纳入范围都已选定且无冲突，既有快照在后续采集和审核变化后仍可复核。

## 命令行与采集

使用 python -m railwatch --db local.sqlite3 source 加 JSON 来源策略登记来源；replay 命令回放本地清单；show scopes 查看当前审核范围；snapshot 配合发布名和项目键冻结证据。具体参数可用 python -m railwatch --help 查看。

采集任务仅面向已登记的来源主机。采集端限制响应大小和跳转，校验内容类型，支持条件请求；SQLite 租约防止多个工作进程同时领取同一任务。失败任务保留错误供重试。
