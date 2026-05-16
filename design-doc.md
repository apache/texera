# Texera AI Workflow Copilot — Design Doc

## 1. 我们要做什么

一个 AI 入口模块，让不会写代码的生物医学研究者通过引导 + 自然语言生成并迭代 Texera 工作流，过程全程透明、可审计、可复现。

不是"另一个 LLM workflow 生成器"，而是**带方法论约束、带 schema 感知、带审计轨迹的科研级 workflow copilot**。

## 2. 目标用户

生物医学研究者（biomedical researchers），具体画像：

- 懂统计 / 懂方法、不写或少写代码
- 数据多为表格型临床/组学数据（cohort、measurements、ICD codes、omics features）
- 产出要写进论文、要被审稿人挑刺、要可重复
- 对"黑盒 AI"天然不信任

这个用户画像决定了我们所有的设计取舍。

## 3. 核心原则（不可妥协）

1. **AI 提议，用户决定。** 任何对 workflow 的修改都必须先以 diff/preview 形式展示，由用户确认后才生效。永远不偷偷改东西。
2. **Review-before-Apply（核心特性）。** 即便是首次生成，workflow 也不会直接落到画布。结果先进入 review panel —— 用户可逐 operator 检视 properties、为缺失的 required 字段补值、阅读对应的 `why`，确认无误后才点 Apply。这把"生成"和"接受"明确切成两步，符合科研用户的谨慎心态。
3. **全程透明。** 每个生成的节点都有 `why` 解释，且 `why` 必须与生成决策在同一次 LLM 调用中产生，不允许事后补编。
4. **不引入假数据。** 不默认推荐 SMOTE 等合成方法；如确需，必须用户显式开启并标注。
5. **可复现可审计。** 每一次生成都留下 manifest（输入 + prompt 版本 + 模型响应 + 最终 workflow），可回放。
6. **不动 Texera 原生代码。** 只做新增模块，通过现有 API 集成。

## 4. 技术架构

### 4.1 整体 pipeline

```
[用户输入]
   │
   ├─→ Wizard 收集结构化选择
   ├─→ Existing Dataset (Texera) / dkNET Dataset 选择
   │
   ▼
[Data Profiler]      ← 关键：让 LLM 看到真实 schema
   │  生成 column 列表、类型、缺失率、样例值
   ▼
[Prompt Builder]
   │  - Operator catalog（自动从 Texera 抽取）
   │  - Data profile（来自上一步）
   │  - 科学框架规则（CRISP-DM / SEMMA / KDD / Custom）
   │  - Guardrails 约束
   │  - Few-shot 示例 + 模板库
   │  - 用户的 wizard 选择 + 框架自由文本
   ▼
[LLM Call]
   │  输出结构化 JSON：{operators: [{operatorID, operatorType, operatorProperties, why}, ...], links: [...]}
   ▼
[Validator]
   │  - JSON schema 合法？
   │  - 所有 operator 存在？参数合法？
   │  - 端口类型匹配、DAG 无环？
   │  - Guardrails 全部满足？
   │  ├─ 失败：把错误回灌 LLM，最多 3 次重试
   │  └─ 通过：进入下一步
   ▼
[Manifest Logger]    ← 关键：审计轨迹
   │  存档本次生成的完整上下文
   ▼
[Review Panel]       ← 关键：review-before-apply
   │  逐 operator 展示 properties + why + missing-required 高亮
   │  用户在此编辑 / 补值 / 拒绝
   ▼
[Texera Workflow Import API]   ← 用户点 Apply 才触发
   │
   ▼
[画布显示 + Guardrails 报告 + Manifest ID]
   │
   ▼
[用户 → 直接跑 / chat 微调 / 手动微调]
```

### 4.2 关键组件说明

**Data Profiler（新增，P0 必做）**
原 plan 没有这一步，但它决定生成质量。流程：

- CSV 上传后跑轻量 profiling：列名、dtype、缺失率、unique count、对每列取 3-5 个样例值
- 对 dkNET 数据集：预先维护 schema metadata，命中即用
- profile 注入 prompt，让 LLM 真正"看见"数据，而不是瞎猜列名

**Operator Catalog（自动化，P0 必做）**
- 从 Texera 源码或 API 自动抽取算子列表 + 参数 + I/O port 类型
- 不允许手工维护 JSON（脆弱、易过期）
- 每次启动服务时同步一次

**Prompt Builder**
模块化拼装，每个 section 独立可测：

| Section | 内容 | 来源 |
|---|---|---|
| System | 角色定义 + 输出格式 | 静态 |
| Operators | 算子目录 | 自动抽取 |
| Methodology | CRISP-DM 等约束 | 静态规则 |
| Guardrails | 防 anti-pattern | 静态规则 |
| Data | 用户数据 profile | Data Profiler |
| Examples | Few-shot workflow | 模板库 |
| Task | 用户的 wizard 选择 + 自由文本 | Wizard |

**Validator**
不是简单 JSON 校验，要做语义校验：
- 结构层：JSON schema、operator 存在性、参数合法性
- 拓扑层：DAG 无环、port 类型匹配、无悬空节点
- 方法论层：guardrails 是否全部满足（见下）

**Guardrails（白盒规则，不依赖 LLM 自觉）**
- 必须有 train/test split 节点出现在 model fitting 之前
- 不允许在 split 之前做 imputation / scaling（防 data leakage）
- 监督学习 workflow 必须有 evaluation 节点
- 必须有缺失值处理节点
- SMOTE / 合成数据方法默认禁用，需用户显式开启

**Manifest Logger**
每次生成存档：
```
{
  manifest_id, timestamp, user_id,
  wizard_inputs, free_text, data_profile_hash,
  prompt_version, prompt_full_text,
  llm_model, llm_response_raw,
  validator_errors_and_retries,
  final_workflow_json,
  guardrails_status
}
```
作为 workflow 的 metadata 附加存储。这是论文复现和审稿应对的基础设施。

## 5. 功能优先级

### P0 — Wizard 生成 + Chat 微调（合并，必做）

> **关键调整**：原 plan 把 chat 放 P1，但 wizard 生成的 v0 几乎不可能一次到位，没有 chat 微调，用户只能手动拖画布，AI 价值减半。Wizard + chat 是一个有机整体。

**Wizard 流程**：
1. Step 1: 分析目标（EDA / 预测 / 清洗 / NLP / Custom）
2. Step 2: 数据来源 —— 两个选项：
   - **Existing Dataset**：从用户已上传到 Texera Datasets 的 CSV 中挑一个，前端会解析为后端可读的 `/<owner>/<dataset>/v<n>/<file>.csv` 路径
   - **dkNET Dataset**：从预置的生物医学数据集目录中挑一个，schema 已经维护好
   - （上一版还考虑过 "Database" 和 "API"，本期落地砍掉 —— 它们对 demo 场景没贡献，且会让 schema profiling 走不一致路径；保留两个互补的 CSV 来源，覆盖"自己的数据"和"公共数据"两类需求）
3. Step 3: 科学方法论（CRISP-DM / SEMMA / KDD / Custom）—— 模板内容用户可编辑，作为 soft guidance 注入 prompt
4. Step 4: Guardrails 配置（默认全开，每条规则展示解释，用户可关）

**生成后体验（review-before-apply 流程）**：
- LLM 返回的 workflow **不立即上画布**，先渲染到 review panel
- 每个 operator 一行：展示 `why`、当前 properties、所有 required 字段中尚未填的会以 "missing" badge 高亮
- 用户可在 review panel 直接编辑 properties（嵌套结构以 JSON 文本编辑）
- 点 Apply 才真正调用 Texera 导入 API，把 workflow 落到画布
- 顶部展示 Guardrails 报告（哪些规则被满足）+ manifest ID（可点开看完整生成记录）

**Chat 微调（合并进 P0）**：
- 用户用自然语言提出修改（"换成 XGBoost"、"加 cross validation"）
- LLM 输出 diff（要删的节点、要加的节点、要改的参数）
- 画布上**高亮预览**（红删绿增 / 参数高亮）
- 用户点确认/拒绝 → 应用
- 每次修改追加到 manifest

### P1 — Auto-fix

Workflow 跑错时：
1. Agent 读取 error log + 当前 workflow + data profile
2. 诊断错误（类型不匹配？缺失列？参数错误？）
3. 以 diff 形式提议修复（仍然走 chat 微调那套 UI）
4. 用户确认后应用

> **本期砍掉的功能**（明确写出来避免回流）：
> - **Reproduce-a-Paper**：价值方向认可，但 PDF/DOI 抽取 → 方法理解 → workflow 映射 链路太长，单期做不透；放到后续迭代
> - **Multi-Agent 任务拆分**：单 agent + chat 微调已足够覆盖 demo 场景；Planner/Worker/Coordinator 三层架构增加的复杂度收益不明显，本期不做

## 6. Evaluation —— 怎么知道我们做得好

> 原 plan 完全缺失。没有这个，三周后改 prompt 改到怀疑人生不知道是变好还是变坏。

### 6.1 Golden test set（动手前先准备）

10-15 个测试用例，覆盖：
- EDA workflow（5 个）
- 预测建模 workflow（5 个）
- 数据清洗 workflow（3 个）

每个 case：`(user_input, data_profile) → expected_workflow_properties`

不是要求生成的 JSON 字节级相同，而是检查关键属性：
- 包含哪些类型的节点
- DAG 拓扑结构是否合理
- Guardrails 是否满足
- 跑通后输出是否符合预期

### 6.2 自动评估指标

| 指标 | 目标 |
|---|---|
| Validator pass rate (首次) | ≥ 70% |
| Validator pass rate (3 retry 后) | ≥ 95% |
| Workflow 跑通率 | ≥ 90% |
| Guardrails 满足率 | 100% |
| Golden test 通过率 | ≥ 80% |

每改一次 prompt 都跑一次完整 eval suite。

### 6.3 用户研究

Demo 阶段邀请 3-5 个真实生物医学研究者过一遍，记录卡点和困惑。

## 7. 差异化（凭什么赢）

1. **Schema-aware generation**：上传数据后真正"看见"列名和分布，而不是瞎猜——这是质量上一个数量级的差距
2. **方法论作为硬约束**：CRISP-DM 不是 prompt 里一句话，而是 validator 强制检查的规则
3. **白盒 Guardrails**：防 data leakage / 缺失 evaluation / 缺失 split / 假数据注入，不依赖 LLM 自觉
4. **可审计 Manifest**：每次生成可回放，直接对接学术伦理与论文复现需求
5. **Review-before-Apply**：首次生成 + 后续 chat 微调全都走"先 review 再落画布"的两步流程，符合科研工作流的谨慎传统
6. **dkNET 深度集成**：预加载数据集 schema，从 wizard 选择到 workflow 生成全链路优化

## 8. Demo 设计

**坚决不用 mock 数据。** 一个真实场景跑通 > 三个 mock 场景。

**主 demo 场景**：用 dkNET 糖尿病队列数据，演示"预测 5 年内并发症发生"完整流程：

1. Wizard：选预测建模 → 选 dkNET 数据集 → 选 CRISP-DM → 默认 guardrails
2. 30 秒内生成完整 workflow（数据加载 → 探索 → 缺失值处理 → 特征工程 → train/test split → 模型训练 → 评估）
3. **Review panel 出现** —— 用户检视每个 operator 的 `why` 与 properties，按需调整，再点 Apply 落到画布
4. 顶部 guardrails 报告全绿、manifest ID 可点开看完整生成记录
5. Chat 微调："把 logistic regression 换成 XGBoost 并加 5-fold CV"
6. Diff 预览 → 确认 → workflow 更新
7. 一键跑通，展示真实结果

## 9. 技术选型

| 项目 | 选择 | 理由 |
|---|---|---|
| 前端 | **Angular 组件，直接嵌入 Texera workspace** | 实际落地后发现：作为独立 React 应用反而要重建 Texera 的 dataset picker / Property Editor / workflow import 链路；以 `ai-wizard-panel` 组件形式嵌入 workspace 可以直接复用这些既有能力，体验也更连续。原 plan 写的"React 独立 wizard"已废弃。 |
| LLM | Claude（默认）| 结构化 JSON + 指令遵循稳定 |
| LLM 抽象 | 前端 `AiWizardService` 收口所有 LLM 调用 | Model-agnostic，便于切换 |
| 数据 profiling | 前端读取上传文件做轻量 profiling | 避开新增 Python 服务的部署成本，复用 Texera 已有的 Datasets / 文件下载 API |
| 集成方式 | Texera 现有 workflow import + Property Editor API | 不动核心代码 |

## 10. 风险与对策

| 风险 | 对策 |
|---|---|
| LLM 生成的 workflow 跑不通 | Validator + retry loop + Guardrails + 真实 schema 注入 |
| Operator catalog 与 Texera 主线脱节 | 自动抽取，每次启动同步 |
| 用户改 chat 后画布乱掉 | Diff/preview UI，用户确认才应用 |
| Demo 当天 LLM API 抖动 | 准备本地缓存的 demo 路径作为 fallback |
| 生物医学评委质疑可重复性 | Manifest 系统现场展示 |
| 时间不够 | P0 + P1 优先做透，后续功能按依赖关系排期 |

## 11. 团队决议事项

1. **功能排期**：P0（wizard + review panel + chat 微调）→ P1（auto-fix）。Reproduce-a-Paper 与 Multi-Agent 本期不做。
2. **LLM 选型**：Claude 起步，但代码层做 model-agnostic 抽象
3. **Wizard 模式**：固定 4 步 + 每步允许自由文本编辑模板
4. **前端形态**：Angular 组件直接嵌入 workspace，不做独立 React 应用
5. **Demo 数据**：dkNET 真实数据 + 用户自有 dataset，糖尿病并发症预测场景，**不用 mock**
6. **Evaluation**：动手第一周内完成 golden test set 搭建，作为后续所有 prompt 改动的回归基线

## 12. 里程碑建议

| 阶段 | 目标 |
|---|---|
| Week 1 | Operator catalog 自动抽取 + Data profiler + Golden test set + Prompt v0 跑通最简单 case |
| Week 2 | Wizard UI（Angular 嵌入版）+ Validator + Manifest + 基础 guardrails；P0 wizard 路径端到端打通到 review panel |
| Week 3 | Review-before-apply 完整体验（per-operator edit、missing-required 高亮）+ Chat 微调 + Diff UI；eval 通过率达标 |
| Week 4 | P1 Auto-fix + dkNET 数据集目录扩充 + Demo 打磨 + 用户研究 |

---

**核心一句话**：把 wizard 生成 + review-before-apply + chat 微调 + manifest 审计 + schema 感知 + 方法论约束这六件事做扎实，单 demo 场景跑通，比堆 5 个半成品功能强得多。