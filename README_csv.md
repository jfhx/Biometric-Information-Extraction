## 一、
我现在给你一个明确时间：1分钟内把检查结果和可复制命令发你。先做我改动文件的语法检查与 lint 检查。

python -m py_compile "app/batch_extract_csv_qwen.py" "app/services/prompt_templates.py"

已完成，并且检查通过了（语法 + linter 都 OK）。
### 可以直接运行：

cd "C:\Users\imcas\Desktop\Biometric Information Extraction"
python -m app.batch_extract_csv_qwen --limit 10

先跑 10 条做验证（会更快）。
确认没问题后跑全量：
cd "C:\Users\imcas\Desktop\Biometric Information Extraction"
python -m app.batch_extract_csv_qwen --limit 0

### 也可以自定义参数（超时、截断长度、输出文件）：

cd "C:\Users\imcas\Desktop\Biometric Information Extraction"
python -m app.batch_extract_csv_qwen ^  
--input-csv "C:\Users\imcas\Desktop\Biometric Information Extraction\don_text_extracted.csv" ^  
--output-excel "C:\Users\imcas\Desktop\Biometric Information Extraction\qwen_extracted_result.xlsx" ^  --output-timing-csv "C:\Users\imcas\Desktop\Biometric Information Extraction\qwen_extracted_timing.csv" ^  --max-chars 12000 ^  
--timeout 180 ^  
--limit 0

### 改了哪些文件
app/services/prompt_templates.py：新增 CSV 专用提示词（你要的字段结构）
app/batch_extract_csv_qwen.py：新增独立批处理脚本（不改原 /extract 接口）

### 运行后会得到
qwen_extracted_result.xlsx
extracted：字段抽取结果
timing：每条 full_text 的处理耗时
summary：全表总耗时、均值、P50/P90/P95、失败数等
qwen_extracted_timing.csv：每条耗时明细（便于你后续画图分析）










## 二、

并发脚本已写好，我再做一次快速语法检查，马上把你直接可运行的命令发你。
python -m py_compile "app/batch_extract_csv_qwen_parallel.py"
可以继续，已经给你写好了并发版脚本，文件是：
app/batch_extract_csv_qwen_parallel.py
语法和 lint 都检查通过。

### 直接运行命令如下：
cd "C:\Users\imcas\Desktop\Biometric Information Extraction"
python -m app.batch_extract_csv_qwen_parallel --limit 20 --workers 2

### 全量跑（3461条）：
cd "C:\Users\imcas\Desktop\Biometric Information Extraction"
python -m app.batch_extract_csv_qwen_parallel --limit 0 --workers 2

### 如果你要压吞吐上限测试（先从 3 开始）：
cd "C:\Users\imcas\Desktop\Biometric Information Extraction"
python -m app.batch_extract_csv_qwen_parallel --limit 0 --workers 3 --retries 1 --retry-wait 1.5

### 会输出：
qwen_extracted_result_parallel.xlsx（extracted/timing/summary 三个 sheet）
qwen_extracted_timing_parallel.csv（每条耗时和状态）

### 建议：
qwen3:235b 先用 workers=2，稳定后试 3，再看失败率和耗时是否更优。
如果报 429/超时，降低并发到 1-2，并适当增大 --timeout（如 180）。













## 三、
改完了，已经可用（语法和 linter 都通过）。
你要的两项都已加上：
每完成 N 条打印进度（默认 100，参数 --progress-every）
--progress-file 持续写进度到本地 CSV
直接运行（推荐）：
cd "C:\Users\imcas\Desktop\Biometric Information Extraction"
python -m app.batch_extract_csv_qwen_parallel ^  
--limit 0 ^  --workers 2 ^  --progress-every 100 ^  
--progress-file "C:\Users\imcas\Desktop\Biometric Information Extraction\progress.csv"

如果先做小样本压测：
python -m app.batch_extract_csv_qwen_parallel --limit 300 --workers 3 --progress-every 100 --progress-file "C:\Users\imcas\Desktop\Biometric Information Extraction\progress_300.csv"

你现在可以实时看两个地方：
终端：会看到 progress: 已完成/总数/失败数/平均耗时/ETA
progress.csv：每次进度点都会追加一行，方便你随时打开看是否卡住。