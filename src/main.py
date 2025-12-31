from fastapi import FastAPI, BackgroundTasks, HTTPException
from typing import Dict
import uuid
from datetime import datetime
import uvicorn
from src import schemas
from src import scraper

app = FastAPI(title="Carrefour Scraper Service API")

# 内存数据库
JOBS_DB: Dict[str, dict] = {}

@app.post("/tasks", response_model=schemas.TaskSubmitResponse, status_code=202)
async def submit_task(request: schemas.TaskSubmitRequest, background_tasks: BackgroundTasks):
    """
    **提交抓取任务**

    根据 `type` 参数的不同，执行不同的抓取策略：

    * **product**: 抓取商品完整详情。
        * **返回字段**: 包含标题、描述、EAN、品牌、分类、高清图片链接 (Image 1-5)、最低价、卖家信息及竞品比价。
        * **价格策略**: 提取全网最低价 (Best Price)。
    
    * **price_check**: 快速价格监测模式。
        * **返回字段**: 仅包含 `Product URL`, `Price`, `Shipping Cost`, `Seller`。
        * **价格策略**: 提取页面当前展示价 。

    * **store**: 店铺/分类遍历模式。
        * **功能**: 遍历指定的分类或店铺链接，翻页抓取所有商品的 URL。
        * **参数**: 需结合 `pages` 参数使用。

    **参数说明**:
    - `urls`: 目标链接列表 (商品链接或店铺链接)。
    - `pages`: (仅 store 模式) 翻页数量。
    """
    if not request.urls:
        raise HTTPException(status_code=400, detail="URL list cannot be empty")

    job_id = str(uuid.uuid4())
    
    # 初始化状态
    JOBS_DB[job_id] = {
        "task_id": job_id,
        "status": "pending",
        "progress": "Initializing...",
        "processed": 0,
        "total": 0,
        "created_at": datetime.now().isoformat(),
        "results_count": 0,
        "results": []
    }

    # 提交到后台执行
    background_tasks.add_task(
        scraper.run_batch_job, 
        request.type, 
        request.urls, 
        request.pages, 
        JOBS_DB, 
        job_id
    )

    return {
        "task_id": job_id,
        "message": "Task submitted successfully",
        "status_url": f"/tasks/{job_id}"
    }

@app.get("/tasks/{task_id}", response_model=schemas.TaskStatusResponse)
async def get_task_status(task_id: str):
    if task_id not in JOBS_DB:
        raise HTTPException(status_code=404, detail="Task not found")
    
    task_data = JOBS_DB[task_id]
    
    # 减少传输量，未完成时不返回 results 列表
    # 创建副本以避免修改原始内存数据
    response_data = task_data.copy()
    if task_data["status"] not in ["completed", "failed"]:
        response_data["results"] = None
        
    return response_data

@app.get("/health")
def health_check():
    return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)