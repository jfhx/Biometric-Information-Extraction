·# Biometric Information Extraction

基于 FastAPI 的生物疾控信息抽取服务，支持上传 `doc/docx/pdf/xlsx/csv/txt`，调用大模型抽取：
病毒名字、国家、时间、占比、感染率、感染人数等字段。

## 1. 安装依赖
```bash
pip install -r requirements.txt
```
如果需要解析 `.doc`，确保已安装 `textract` 及其系统依赖。
如果需要解析 `.pdf`，确保已安装 `pdfplumber`（已写入 requirements.txt）。

## 2. 环境变量
复制 `.env.example` 到 `.env`，填入模型配置：
```bash
LLM_API_KEY=你的API_KEY
LLM_MODEL=qwen-plus
```

## 3. 启动 （用于开发环境）
```bash
uvicorn app.main:app --reload
```

### 生产环境启动命令（参考）：
去掉--reload，并指定监听地址和端口（让外网能访问）：
```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```


## 4. 接口
`POST /extract`  
表单字段：
- `file`：上传文件
- `description`：可选文本描述
Query 参数：
- `output`：`json`（默认）或 `xlsx`

示例：
```bash
curl -X POST "http://127.0.0.1:8000/extract" ^
  -F "file=@test_files/传染病毒快讯.csv" ^
  -F "description=2024年1月某国流感监测"

下载 xlsx：
```bash
curl -X POST "http://127.0.0.1:8000/extract?output=xlsx" ^
  -F "file=@test_files/传染病毒快讯.csv" ^
  -F "description=2024年1月某国流感监测" ^
  -o result.xlsx
```

```bash
curl.exe -X POST "http://127.0.0.1:8000/extract?output=xlsx" `
  -F "file=@test_files/提取信息示例.docx" `
  -F "description=示例" `
  -o result.xlsx
```

```bash
curl.exe -X POST "http://127.0.0.1:8000/extract" `
  -F "file=@test_files/who-rapid-risk-assessment_chikungunya-virus_global_v1.pdf" `
  -F "description=示例"
  -o  result_7.xlsx


## 5. 输出格式
```json
{
  "file_name": "xxx.csv",
  "file_type": "csv",
  "records": [
    {
      "virus_name": "xxx",
      "strain": "2.3.4.4b",
      "subtype": "D1.1",
      "country": "xxx",
      "location": "xxx",
      "time": "2024-01",
      "transmission_process": "与受感染家禽接触",
      "proportion": "12%",
      "infection_rate": "3.5%",
      "infection_count": "1234",
      "severity": "扩散中",
      "extra_fields": {},
      "evidence": "原文片段"
    }
  ],
  "raw_summary": "可选摘要",
  "model_provider": "qwen",
  "model_name": "qwen-plus"
}
```

## 6. 模型提示词 & 配置
提示词在 `app/services/prompt_templates.py`。
运行时可通过 `.env` 修改温度、top_p、max_tokens。

## 7. 微调说明
`app/services/finetune.py` 提供了微调流程占位实现，
真正微调需要对接厂商微调接口或离线训练脚本。



##  备注：
这是正常现象：你访问的是根路径 /，项目没有定义这个路由，所以返回 404。服务已经启动成功了。
正确访问方式：
打开文档页： http://127.0.0.1:8000/docs 
调用接口：POST /extract
如果你要测试 xlsx 下载，用这条：
curl.exe -X POST "http://127.0.0.1:8000/extract?output=xlsx" ^  -F "file=@test_files/提取信息示例.docx" ^  -F "description=示例" ^  -o result_5.xlsx

```bash

curl.exe -X POST "http://127.0.0.1:8000/extract?output=xlsx" `
  -F "file=@test_files/提取信息示例.docx" `
  -F "description=示例" `
  -o result.xlsx
```








### 全量进行跑和运行的命令（本地 Windows）

```bash
python -m app.batch_extract_csv_qwen_parallel 
--limit 0
 --workers 2 
--progress-every 100 
--progress-file "C:\Users\imcas\Desktop\Biometric Information Extraction\progress_who.csv"
```


你现在可以实时看两个地方：
终端：会看到 progress: 已完成/总数/失败数/平均耗时/ETA
progress.csv：每次进度点都会追加一行，方便你随时打开看是否卡住。

先跑小样本验证（10条）：
```bash
python -m app.batch_extract_csv_qwen_parallel --limit 10 --workers 2
```

带标准化参考表的全量命令：
```bash
python -m app.batch_extract_csv_qwen_parallel ^
  --limit 0 ^
  --workers 2 ^
  --progress-every 100 ^
  --progress-file "C:\Users\imcas\Desktop\Biometric Information Extraction\progress_who.csv" ^
  --dict-xlsx "C:\Users\imcas\Desktop\Biometric Information Extraction\dict_country_global_all.xlsx" ^
  --dict-pathogen-xlsx "C:\Users\imcas\Desktop\Biometric Information Extraction\dict_pathogen_feature.xlsx" ^
  --dict-host-xlsx "C:\Users\imcas\Desktop\Biometric Information Extraction\dict_host_tag.xlsx"
```

---

## 8. 新增功能：日期拆分 + 标准化（国家/省份/病原体/宿主）

### 8.1 日期拆分
输出 Excel 中 `start_date` 后自动追加 `start_date_year`、`start_date_month`、`start_date_day`；
`end_date` 后追加 `end_date_year`、`end_date_month`、`end_date_day`。
- 例如 `2026-01-16` → year=`2026`, month=`01`, day=`16`
- 部分日期 `2025-12` → year=`2025`, month=`12`, day=``（空）

### 8.2 国家/省份标准化
通过 `--dict-xlsx` 参数指定标准化参考表 `dict_country_global_all.xlsx`，
自动将大模型提取的 `original_country`、`original_province`、`spread_country`、`spread_province`
与参考表中的 `country`（含 `country_full_name`）和 `province` 字段进行匹配，输出标准化名称。

### 8.3 病原体标准化
通过 `--dict-pathogen-xlsx` 参数指定参考表 `dict_pathogen_feature.xlsx`，
自动匹配大模型提取的 `pathogen`，输出标准化的 `pathogen`、`pathogen_rank_1`、`pathogen_rank_2`。

匹配优先级：
1. **pathogen**（最具体，如 `FLU_A_H5N1`）— 优先级最高
2. **pathogen_rank_2**（如 `FLU_A`）— 优先级第二
3. **pathogen_rank_1**（最笼统，如 `FLU`）— 优先级最后

同时支持通过 `pathogen_name`（如 "Influenza A H5N1"）进行人类可读名称匹配。
匹配时自动处理连字符/下划线/大小写差异（如 `MERS-CoV` → `mers_cov`）。

### 8.4 宿主标准化
通过 `--dict-host-xlsx` 参数指定参考表 `dict_host_tag.xlsx`，
自动匹配大模型提取的 `host`，输出 `host_rank_1`（大类）和 `host_rank_2`（小类）。

- `host` — 保留大模型提取的原始值
- `host_rank_1` — 宿主大类（如 Human、Mammal、Avian、Arthropod 等）
- `host_rank_2` — 宿主小类（如 Dove、Mosquito、Pig 等）

### 8.5 未匹配记录
未在参考表中找到的国家/省份/病原体/宿主名称统一记录到 `*_unmatched.txt` 文件中。
可通过 `--unmatched-file` 指定输出路径，默认在输出 Excel 同目录下生成。

### 新增 CLI 参数
| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--dict-xlsx` | 国家/省份标准化参考表路径 | 本地: `dict_country_global_all.xlsx`；集群: 需手动指定 |
| `--dict-pathogen-xlsx` | 病原体标准化参考表路径 | 本地: `dict_pathogen_feature.xlsx`；集群: 需手动指定 |
| `--dict-host-xlsx` | 宿主标准化参考表路径 | 本地: `dict_host_tag.xlsx`；集群: 需手动指定 |
| `--unmatched-file` | 未匹配名称输出文件路径 | 空（自动生成在输出 Excel 同目录） |

---

## 9. 集群服务器运行（PBS）

PBS 脚本文件：`run_bio_task.pbs`

### 提交任务
```bash
qsub run_bio_task.pbs
```

### 查看任务状态
```bash
qstat -u sunxiuqiang
```

### 删除任务
```bash
qdel <JOB_ID>
```

### 查看日志
```bash
tail -f /data7/sunxiuqiang/Biometric_Information_Extraction/task_run.log
tail -f /data7/sunxiuqiang/Biometric_Information_Extraction/task_error.log
```

### 查看实时进度
```bash
cat /data7/sunxiuqiang/Biometric_Information_Extraction/progress_test.csv
```

### 查看未匹配的国家/省份/病原体/宿主
```bash
cat /data7/sunxiuqiang/Biometric_Information_Extraction/unmatched_names_test.txt
```

### PBS 脚本关键参数说明
- `#PBS -q fat` — 使用 fat 队列（空闲节点多，优先运行）
- `#PBS -l nodes=node01.chess:ppn=100` — 使用 node01 节点，100 核
- `#PBS -l walltime=100:00:00` — 最大运行时间 100 小时
- `--workers 95` — 并发线程数（集群上可调大）
- `--dict-xlsx` — 指向集群上的 `dict_country_global_all.xlsx` 路径
- `--dict-pathogen-xlsx` — 指向集群上的 `dict_pathogen_feature.xlsx` 路径
- `--dict-host-xlsx` — 指向集群上的 `dict_host_tag.xlsx` 路径

> **注意：** 需要把以下三个 xlsx 文件也上传到集群的项目目录下：
> - `dict_country_global_all.xlsx`
> - `dict_pathogen_feature.xlsx`
> - `dict_host_tag.xlsx`

---

## 10. 注意事项

- **DeepSeek-V3 模型**：使用 DeepSeek-V3 时必须**退出 Clash 代理**，否则无法连接模型 API。运行程序前请确认已关闭代理。
- 如果报 429/超时，降低并发到 1-2，并适当增大 `--timeout`（如 180）。
- 本地推荐 `--workers 2`，集群可根据节点核数调大。