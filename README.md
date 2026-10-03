# delimited-stream-import-go

面向大体积分隔文本（CSV/TSV/自定义分隔符）的**流式、可恢复、并发安全**导入库。
核心包：`streamimport`。

```go
import "github.com/Diudiuseven/delimited-stream-import-go/streamimport"
```

## 快速开始

```go
// 1) 从样本自动识别方言（分隔符/引号/换行/表头）
d, replay, err := streamimport.SampledDialect(file, 0)
if err != nil {
    // *ErrAmbiguousDialect：证据不足，必须显式覆盖，库不会静默猜测
    var amb *streamimport.ErrAmbiguousDialect
    if errors.As(err, &amb) { /* 用 DialectOverride 显式指定 */ }
}

// 2) 构造导入器
im, err := streamimport.NewImporter(streamimport.ImporterConfig{
    Reader: streamimport.ReaderConfig{
        Dialect:        d,
        MaxRecordBytes: 1 << 20, // 单条记录上限，超出报 *ErrRecordTooLarge
    },
    Schema:    streamimport.Schema{Policy: streamimport.RaggedReject},
    BatchSize: 500,
    Sink:      mySink,                    // 原子提交一批记录
    Store:     &streamimport.FileCheckpointStore{Path: "import.ckpt"},
})

// 3) 运行；可随时通过 im.Progress().Snapshot() 查询进度
err = im.Run(ctx, replay)
```

并发分片（输入可随机访问时）：

```go
pi, _ := streamimport.NewParallelImporter(streamimport.ParallelConfig{
    ImporterConfig: cfg, // 同上
    Shards:         8,
})
err = pi.Run(ctx, file, fileSize) // file 需实现 io.ReaderAt
```

## 方言识别规则

`DetectDialect(sample)` 的判定规则（同样输入必然得到同样结果）：

- **BOM**：分析前先剥离 UTF-8 BOM（`EF BB BF`），不参与任何统计。
- **换行风格**：统计 `CRLF` / `LF` / `CR` 出现次数，占比 ≥ 90% 者胜出，
  否则报告 `Mixed`。解析器对三种风格及混用都透明处理。
- **分隔符**：候选集固定为 `,` `\t` `;` `|` `:`（即平局时的优先级）。
  对每个候选统计"引号之外每行字段数"，取众数行数最多、且字段数更多者。
  无候选能产生一致列数，或前两名打平 → 返回 `*ErrAmbiguousDialect`，
  **绝不静默猜测**。
- **引号**：候选 `"` 和 `'`。仅当某字符出现在字段起始位置 ≥ 2 次，
  或 ≥ 1 次且引号内出现分隔符时，才认定为引号；否则 `Quote = 0`（禁用引号语义）。
- **表头**：首行与其余行列数一致，且存在某列"首行非数值而后续行出现数值"
  时判定有表头；证据不足一律按无表头处理。

任何字段都可以用 `DialectOverride` 显式覆盖（`Dialect.Apply`），
覆盖优先于识别结果。

## 内存边界

- 解析器使用固定 64 KiB 读缓冲 + 单条记录缓冲，内存占用**不随输入长度增长**。
- 单条记录上限由 `MaxRecordBytes` 控制（必填）。超限产生可区分的
  `*ErrRecordTooLarge`（含行号与偏移）：严格模式立即失败；宽松模式
  （`Lenient: true`）跳过该记录继续，位置跟踪仍然正确。
- 并发模式的内存上界为 `Shards × BatchSize × MaxRecordBytes` 量级
  （每分片一个有界 channel + 提交侧一个批次缓冲）。

## 引号语义

- 引号字段内可以出现分隔符、任意风格换行、以及转义引号，
  均不破坏记录边界。
- 转义规则：`Escape = 0` 时用双写引号（`""`）；`Escape` 非零（如 `\`）
  时该字符使下一字节失去特殊含义。
- 非引号字段中间出现引号、闭引号后跟随非分隔符/换行字符 →
  `*ErrBareQuote`（带行号与字节偏移）。
- 输入在引号字段内结束 → `*ErrUnclosedQuote`（指向字段起始位置）。
- 宽松模式下上述错误及超限记录会被跳过并经 `OnError` 回调上报，
  流继续；未闭合引号因流已结束而终止。

## 换行与 BOM

- 开头的 UTF-8 BOM 被剥离，绝不进入字段内容；所有记录偏移量以
  BOM 之后的首字节为 0，因此带不带 BOM 的文件偏移语义一致，续跑不受影响。
- 同一文件内混用 `LF` / `CRLF` / `CR` 不会造成记录错位或字段粘连；
  换行字符本身不进入字段（引号字段内的换行除外，那是内容）。
- 只含换行符的空行被跳过。

## 表头映射与列数校验

- `Dialect.HasHeader` 时首行作为表头；`Schema.Columns` 显式给出时优先，
  且会校验文件表头列数是否一致。
- 列数不一致的记录按 `Schema.Policy` 处理：
  - `RaggedReject`：严格模式失败 / 宽松模式跳过（`*ErrColumnCount`）。
  - `RaggedPad`：短记录补 `""`；多出的字段默认仍拒绝，只有同时设置
    `TruncateExtra: true` 才截断（截断会经 `OnError` 上报）。
  - `RaggedDrop`：丢弃该记录并计数、上报。
- 任何模式都不会静默丢数据：补齐是显式的，截断需双重确认，丢弃必上报。
- `Schema.Map(rec)` 把记录映射为 `列名 -> 值`。

## 并发分片

- `ParallelImporter` 把 `[checkpoint, size)` 切成 N 个分片，边界通过对
  换行扫描（跟踪引号/转义状态）并对候选边界做"试解析 + 列数校验"来对齐，
  落在引号字段内的候选会被拒绝。病态输入（巨大引号字段含大量换行）建议
  `Shards: 1` 退化为精确顺序扫描。
- 各分片并发解析，提交协程**严格按输入顺序**合批提交。
- 任一分片失败：该分片未提交的残余批次被丢弃，失败分片之后的分片记录
  绝不会被提交——已提交部分恒为输入的合法前缀，无部分提交。
- `Progress().Snapshot()` 可随时查询已读/已提交/已跳过记录数、错误数与
  已读字节数，全部使用原子计数，并发安全。

## 恢复语义

- 每个批次经 `Sink.CommitBatch` **原子**提交后，才把
  `{Offset, Records}` 写入 `CheckpointStore`。
- 进程中断、`ctx` 取消或超时后，重新运行会从最近一次已提交偏移继续：
  - 已提交记录不会重复提交（从检查点偏移处续读）；
  - 未提交记录不会被跳过（未 flush 的批次随失败一起丢弃）；
  - 不会有半写入状态（`FileCheckpointStore` 先写临时文件、fsync、
    再 rename，并 fsync 目录）。
- 注意：检查点写入失败会使本次运行失败，但已提交批次仍是合法前缀。
- 已知限制：带表头且未显式配置 `Schema.Columns` 时，从检查点续跑无法
  恢复表头内容（列名映射不可用，列数校验依赖显式 `ExpectedWidth`）。
  需要表头语义时请显式配置 `Schema.Columns`。

## 错误类型一览

| 类型 | 含义 | 可恢复（宽松模式跳过） |
|---|---|---|
| `*ErrRecordTooLarge` | 单条记录超过 `MaxRecordBytes` | 是 |
| `*ErrBareQuote` | 畸形引号（带位置） | 是 |
| `*ErrUnclosedQuote` | 输入在引号字段内结束 | 流已结束，终止 |
| `*ErrColumnCount` | 列数与期望不符 | 取决于策略 |
| `*ErrAmbiguousDialect` | 识别证据不足，需显式覆盖 | — |

`IsRecoverable(err)` 可判断错误是否属于可跳过的记录级错误。

## 测试

```sh
go test ./...        # 单元测试
go test -race ./...  # 竞态检测
```

覆盖：畸形引号、未闭合字段、列数变化（补齐/丢弃/拒绝）、BOM、
混合换行、超限记录、并发分片顺序性、分片失败无部分提交、
取消/失败后的检查点续跑、检查点存储原子性。
