import logging

from fastapi import FastAPI

from app.api.routes import router
from app.core.config import settings
from app.core.logging import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


# 这个app就是命令中最后的app（应用实例） 
# uvicorn app.main:app --reload 这句话的意思是：
# 1. app.main:app 表示应用的模块和实例
# 2. --reload 表示启用自动重载，当代码变化时自动重启服务器
# 3. 这样就可以在代码修改后自动重启服务器，方便开发
# 自动重新启动 ，模块与实例名字
app = FastAPI(title=settings.APP_NAME)
app.include_router(router)


@app.on_event("startup")
async def on_startup() -> None:
    logger.info("App started")
