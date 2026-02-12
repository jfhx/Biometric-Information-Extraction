import os

from dotenv import load_dotenv

load_dotenv()

"""
科技云：大模型

科技云查看可用模型
https://uni-api.cstcloud.cn/doc/llm/
API URL ="https://uni-api.cstcloud.cn/v1/chat/completions": 
TOKEN ="9883846189f899d705b276b380df890e8faa0f1564a4653910b1dd7d8d0d466e";
MODEL_NAME ="qwen3:235b";
"""



class Settings:
    APP_NAME = os.getenv("APP_NAME", "Biometric Info Extraction")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_DIR = os.getenv("LOG_DIR", "logs")

    LLM_PROVIDER = os.getenv("LLM_PROVIDER", "qwen")
    LLM_BASE_URL = os.getenv(
        # "LLM_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1"
        # "LLM_BASE_URL", "https://uni-api.cstcloud.cn/v1"
        "LLM_BASE_URL", "http://159.226.80.101:1045/v1"   #可使用魔搭社区的模型 # 自己机房里的模型API，不需要Key #Deepseek-V3
    )
    LLM_API_KEY = os.getenv("LLM_API_KEY", "").strip()
    # LLM_API_KEY = os.getenv("LLM_API_KEY", "sk-36ad5c65085e43aea2ca888ffc101709")
    # LLM_API_KEY = os.getenv("LLM_API_KEY", "9883846189f899d705b276b380df890e8faa0f1564a4653910b1dd7d8d0d466e")
    # LLM_API_KEY = os.getenv("LLM_API_KEY", "4093c10a5dc6cee213a71c60d549e0c2696009aa225af33c5a83fcc589ae4c61")   #科技云密钥会有监控
    # LLM_MODEL = os.getenv("LLM_MODEL", "qwen-plus")
    # LLM_MODEL = os.getenv("LLM_MODEL", "qwen3:235b")
    LLM_MODEL = os.getenv("LLM_MODEL", "Deepseek-V3")
   
    LLM_TIMEOUT = float(os.getenv("LLM_TIMEOUT", "60"))
    LLM_TEMPERATURE = float(os.getenv("LLM_TEMPERATURE", "0.2"))
    LLM_TOP_P = float(os.getenv("LLM_TOP_P", "0.8"))
    LLM_MAX_TOKENS = int(os.getenv("LLM_MAX_TOKENS", "1024"))

    MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", "20"))
    MAX_TABLE_ROWS = int(os.getenv("MAX_TABLE_ROWS", "200"))
    MAX_TEXT_CHARS = int(os.getenv("MAX_TEXT_CHARS", "30000"))


settings = Settings()
