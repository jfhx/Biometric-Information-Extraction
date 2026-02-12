# Biometric Information Extraction

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
打开文档页：  
调用接口：POST /extract
如果你要测试 xlsx 下载，用这条：
curl.exe -X POST "http://127.0.0.1:8000/extract?output=xlsx" ^  -F "file=@test_files/提取信息示例.docx" ^  -F "description=示例" ^  -o result_5.xlsx

```bash

curl.exe -X POST "http://127.0.0.1:8000/extract?output=xlsx" `
  -F "file=@test_files/提取信息示例.docx" `
  -F "description=示例" `
  -o result.xlsx