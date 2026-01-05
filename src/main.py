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

    * **product**: 抓取商品完整详情 (包含图片、描述等)。
    
    * **repricing** : 改价采集模式。
        * **策略**: 提取当前页面**最低价**。
        * **返回**: URL, 最低价(Price), 运费, 店铺, 以及价格排序第2、第3的竞品信息。
        
    * **listing_price** : 上架价格采集模式。
        * **策略**: 提取当前页面**最低价**。
        * **返回**: 仅包含 URL, 最低价(Price), 运费, 店铺。

    * **store**: 店铺/分类遍历模式。
    """
    if not request.urls:
        raise HTTPException(status_code=400, detail="URL list cannot be empty")

    job_id = str(uuid.uuid4())
    
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
    response_data = task_data.copy()
    if task_data["status"] not in ["completed", "failed"]:
        response_data["results"] = None
        
    return response_data

@app.get("/health")
def health_check():
    return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)