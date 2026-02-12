import requests
import json

# ===================== 核心配置（替换成你的科技云参数） =====================
API_URL = "https://uni-api.cstcloud.cn/v1/chat/completions"  # 科技云API地址
TOKEN = "9883846189f899d705b276b380df890e8faa0f1564a4653910b1dd7d8d0d466e"  # 你的科技云TOKEN
MODEL_NAME = "qwen3:235b"  # 要测试的模型名

# ===================== 测试用的生物信息提取请求 =====================
# 简单的测试文本（模拟你的docx内容）
test_text = """2023年11月，美国纽约州报告了1200例SARS-CoV-2的Omicron亚型感染病例，该病原体属于冠状病毒科，相关研究发表于DOI:10.1038/s41586-023-06600-9。"""

# 构造大模型请求体（适配OpenAI兼容接口格式）
payload = {
    "model": MODEL_NAME,
    "messages": [
        {
            "role": "user",
            "content": f"""请从以下文本中提取生物信息，提取字段包括：病毒名称、病原体类型、亚型、涉及国家/地区、时间、感染人数。
要求：1. 字段不存在标注为“无”；2. 结果以JSON格式输出。
文本内容：{test_text}"""
        }
    ],
    "temperature": 0.1,  # 低随机性，保证提取结果稳定
    "max_tokens": 1000   # 输出长度上限
}

# 请求头（携带TOKEN认证）
headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

# ===================== 发送请求+结果解析 =====================
def test_qwen3_235b():
    try:
        # 发送POST请求调用大模型
        response = requests.post(
            url=API_URL,
            headers=headers,
            data=json.dumps(payload),
            timeout=30  # 超时时间30秒，避免卡壳
        )
        
        # 打印响应状态码（排查连接问题）
        print(f"响应状态码：{response.status_code}")
        
        # 状态码200=请求成功，解析结果
        if response.status_code == 200:
            result = response.json()
            # 提取大模型的回复内容
            extract_result = result["choices"][0]["message"]["content"]
            print("\n===== 大模型提取结果 =====")
            print(extract_result)
        # 非200状态码=配置/权限错误，打印详细错误信息
        else:
            print(f"\n===== 调用失败，错误信息 =====")
            print(f"错误状态码：{response.status_code}")
            print(f"错误详情：{response.text}")
    
    # 捕获连接错误（URL无效、网络不通）
    except requests.exceptions.ConnectionError:
        print("\n❌ 连接失败：API URL无效或科技云服务器无法访问")
    # 捕获TOKEN认证错误
    except requests.exceptions.HTTPError as e:
        print(f"\n❌ 认证失败：TOKEN无效/过期或权限不足，错误详情：{e}")
    # 捕获超时错误
    except requests.exceptions.Timeout:
        print("\n❌ 请求超时：大模型响应超过30秒，请检查网络或模型负载")
    # 其他未知错误
    except Exception as e:
        print(f"\n❌ 未知错误：{str(e)}")

# 执行测试
if __name__ == "__main__":
    print("开始测试科技云qwen3:235b大模型调用...")
    test_qwen3_235b()